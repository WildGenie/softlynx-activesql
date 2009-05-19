using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;


namespace Softlynx.SimpleRemoting
{
    
    /// <summary>
    /// Holds input parameters and output results passed to MessageHandler delegate
    /// </summary>
    public class RemotingParams  
    { 
        Dictionary<string,string> input=new Dictionary<string,string>();
        Dictionary<string,string> output=new Dictionary<string,string>();
        public Dictionary<string,string> Input  {get { return input;}}
	    public Dictionary<string,string> Output {get { return output;}}
    }


    
    /// <summary>
    /// Each remote connect produce the handler thread the delegate is called from upon each new query
    /// </summary>
    /// <param name="parameters"></param>
    public delegate void MessageHandler(RemotingParams parameters);

    /// <summary>
    /// Server side remoting component in form of TCP listner handles client connection and queries.
    /// Each client connection is running from separate thread.
    /// </summary>
    public class Server:IDisposable
    {
        /// <summary>
        /// Each key value pair is limited to this max alllowed value length
        /// </summary>
        const int MAX_PARAM_LENGTH = 1024 * 1024;
        
        private bool terminated = false;
        private List<Socket> ActiveClients = new List<Socket>();
        private MessageHandler handler;
        private TcpListener listner = null;
        private Thread AsyncRun = null;
        
        /// <summary>
        /// Instantiate new server handler binds to specific TCP end point 
        /// and user supplied MessageHandler delegate
        /// </summary>
        /// <param name="binding">TCP IP endpoint (listen address and port)</param>
        /// <param name="Handler">User supplied callback delegate</param>
        public Server(IPEndPoint binding, MessageHandler Handler)
        {
            handler = Handler;
            listner = new TcpListener(binding);
        }

        /// <summary>
        /// Starts synchronous client connection handling
        /// </summary>
        public void Run()
        {
            listner.Start();
            try
            {
                while (!terminated)
                {
                    ThreadPool.QueueUserWorkItem(new WaitCallback(
                            delegate(object o)
                            {
                                lock (ActiveClients) { ActiveClients.Add((Socket)o); }
                                try
                                {
                                    HandleClient(o);
                                }
                                finally
                                {
                                    lock (ActiveClients) { ActiveClients.Remove((Socket)o); }
                                }
                            }), listner.AcceptSocket());

                }
                listner.Stop();
            }
            catch (InvalidOperationException) { }
            while (ActiveClients.Count>0)
                lock (ActiveClients)
                     foreach (Socket s in ActiveClients) s.Disconnect(false);
        }

        private void Terminate()
        {
            terminated = true;
            listner.Stop();
            if (AsyncRun != null)
                AsyncRun.Join();
        }
        

        /// <summary>
        /// Main network dialog for each new connection
        /// </summary>
        /// <param name="socket"></param>
        private void HandleClient(object socket)
        {
            try
            {
                using (Stream strm = new NetworkStream((Socket)socket, false))
                {
                    StreamReader rd = new StreamReader(strm);
                    StreamWriter wr = new StreamWriter(strm);
                    wr.WriteLine("200 READY");wr.Flush();
                    RemotingParams rp = new RemotingParams();
                    while (!rd.EndOfStream)
                    {
                        string line = rd.ReadLine();
                        int sep=line.LastIndexOf(' ');
                        if (line == string.Empty)
                        {
                            wr.WriteLine("204 BYE"); wr.Flush();
                            break;
                        }

                        string field = sep<0?null:line.Substring(0, sep);
                        string value = line.Substring(sep+1);
                        if (field == null) // command passed in value
                        {
                            if (value == "EXEC")
                            {
                                try
                                {
                                    handler(rp);
                                }
                                catch (Exception ex)
                                {
                                    string msg = ex.Message;
                                    wr.WriteLine("500 EXCEPTION {0}",msg.Length);
                                    wr.Write(msg);
                                    wr.Flush();
                                    continue;
                                }
                                wr.WriteLine("200 OK {0}",rp.Output.Count);
                                foreach (KeyValuePair<string, string> kvp in rp.Output)
                                {
                                    wr.WriteLine("{0} {1}",
                                    kvp.Key.Replace('\r', ' ').Replace('\n', ' '),
                                    kvp.Key.Length);
                                    wr.Write(kvp.Value);
                                }
                                wr.Flush();
                                rp.Input.Clear();
                                rp.Output.Clear();
                                continue;
                            }
                            wr.WriteLine("404 UNKNOWN"); wr.Flush();
                            continue;
                        }
                        int blocklen = 0;
                        if (!int.TryParse(value, out blocklen))
                        {
                            wr.WriteLine("400 MAILFORMED"); wr.Flush();
                            continue;
                        }
                        if (blocklen > MAX_PARAM_LENGTH)
                        {
                            wr.WriteLine("406 OVERSIZE"); wr.Flush();
                            break;
                        }
                        char[] buf=new char[blocklen];
                        rd.ReadBlock(buf, 0, blocklen);
                        rp.Input[field] = new string(buf);
                        wr.WriteLine("202 OK"); wr.Flush();
                    }
                    
                    strm.Close();
                }
            }
            finally
            {
                ((Socket)socket).Disconnect(false);
            }
        }

        public void RunAsync()
        {
            if (AsyncRun != null) return;
            AsyncRun = new Thread(new ThreadStart(Run));
            AsyncRun.Start();
        }
        
        public void Dispose()
        {
            Terminate();
        }
    }
}
