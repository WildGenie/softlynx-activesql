using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;


namespace Softlynx.SimpleRemoting
{
    /// <summary>
    /// Specifies the remoting phase for the handler delegate processed
    /// </summary>
    public enum RemotingPhase { 
        /// <summary>
        /// Client has just been connected.
        /// </summary>
        Established, 
        /// <summary>
        /// New client query
        /// </summary>
        Query, 
        /// <summary>
        /// Client disconnected for any reason
        /// </summary>
        Disposing };
    
    /// <summary>
    /// Holds input parameters and output results passed to MessageHandler delegate
    /// </summary>
    public class RemotingParams  
    {
        /// <summary>
        /// Remoting phase for the handler delegate processed
        /// </summary>
        public RemotingPhase Phase = RemotingPhase.Established;
        Dictionary<string,string> input=new Dictionary<string,string>();
        Dictionary<string,string> output=new Dictionary<string,string>();
        Hashtable session = new Hashtable();

        /// <summary>
        /// Holds key value pairs passed from remote side. Not the subject to be midified.
        /// </summary>
        public Dictionary<string,string> Input  {get { return input;}}
        /// <summary>
        /// Holds key value pairs will be passed back to remote side as a part of single query.
        /// </summary>
	    public Dictionary<string,string> Output {get { return output;}}

        /// <summary>
        /// Client session state object.
        /// It is safe place to keep client connection related data.
        /// </summary>
        public Hashtable Session { get { return session; } }
    }


    
    /// <summary>
    /// Each remote connect produce the handler thread the delegate is called from upon each new query.
    /// Any excaption will be interceped on lower level and reported to remote side.
    /// </summary>
    /// <param name="parameters">Read from Input property and modify the Output</param>
    public delegate void MessageHandler(RemotingParams parameters);

    /// <summary>
    /// Server side remoting component in form of TCP listner handles client connection and queries.
    /// Each client connection is running from separate thread.
    /// <example>
    /// <code>
    /// using Softlynx.SimpleRemoting;
    /// ....
    /// Server srv = new Server(new IPEndPoint(IPAddress.Any, 9090), new MessageHandler(MyHandler));
    /// srv.RunAsync(); // either srv.Run();
    /// ....
    /// protected void MyHandler(RemotingParams parameters)
    /// {
    ///             if (parameters.Phase == RemotingPhase.Established)
    ///        {
    ///            // initialize the session
    ///            return;
    ///        }
    ///        if (parameters.Phase == RemotingPhase.Disposing)
    ///        {
    ///            // dispose the session
    ///            return;
    ///        }
    ///        if (parameters.Phase != RemotingPhase.Query)
    ///        {
    ///            // should never happened
    ///            throw new ApplicationException("Unknown remoting phase");
    ///        }
    /// if (parameters.Input["KEY"]=="VALUE")
    /// {...}
    /// parameters.Output.Add("Response1","ResponseValue1");
    /// }
    /// </code>
    /// </example>
    /// </summary>
    public class Server:IDisposable
    {
        /// <summary>
        /// Each key value pair is limited to this max alllowed value length.
        /// Default value is 1Mb.
        /// </summary>
        public static int MAX_PARAM_LENGTH = 1024 * 1024;
        /// <summary>
        /// Protocol version identifier.
        /// Currently "1.0".
        /// </summary>
        public const string VERSION = "1.0";
        
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
                                    ((Socket)o).Close();
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
            catch (SocketException) { }
            while (ActiveClients.Count>0)
                lock (ActiveClients)
                     foreach (Socket s in ActiveClients) s.Close();
        }

        /// <summary>
        /// Stops client handlin either sync or async.
        /// </summary>
        public void Terminate()
        {
            terminated = true;
            listner.Stop();
            if (AsyncRun != null)
            {
                AsyncRun.Join();
                AsyncRun = null;
            }
        }
        

        /// <summary>
        /// Main network dialog for each new connection
        /// </summary>
        /// <param name="socket"></param>
        private void HandleClient(object socket)
        {
            RemotingParams rp = new RemotingParams();
            try
            {
                using (Stream strm = new NetworkStream((Socket)socket, false))
                {
                    StreamReader rd = new StreamReader(strm);
                    StreamWriter wr = new StreamWriter(strm);
                    rp.Phase = RemotingPhase.Established;
                    handler(rp);
                    wr.WriteLine("200 READY {0}",VERSION); wr.Flush();
                    rp.Phase = RemotingPhase.Query;
                    while (!rd.EndOfStream)
                    {
                        string line = rd.ReadLine();
                        int sep = line.LastIndexOf(' ');
                        if (line == string.Empty)
                        {
                            wr.WriteLine("204 BYE"); wr.Flush();
                            break;
                        }

                        string field = sep < 0 ? null : line.Substring(0, sep);
                        string value = line.Substring(sep + 1);
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
                                    string msg = ex.ToString();
                                    wr.WriteLine("500 EXCEPTION {0}", msg.Length);
                                    wr.Write(msg);
                                    wr.Flush();
                                    continue;
                                }
                                wr.WriteLine("200 OK {0}", rp.Output.Count);
                                foreach (KeyValuePair<string, string> kvp in rp.Output)
                                {
                                    wr.WriteLine("{0} {1}",
                                    kvp.Key.Replace('\r', ' ').Replace('\n', ' '),
                                    kvp.Value.Length);
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
                        try
                        {
                            blocklen = int.Parse(value);
                        }
                        catch 
                        {
                            wr.WriteLine("400 MAILFORMED"); wr.Flush();
                            continue;
                        }
                        if (blocklen > MAX_PARAM_LENGTH)
                        {
                            wr.WriteLine("406 OVERSIZE"); wr.Flush();
                            break;
                        }
                        char[] buf = new char[blocklen];
                        rd.ReadBlock(buf, 0, blocklen);
                        rp.Input[field] = new string(buf);
                        wr.WriteLine("202 OK"); wr.Flush();
                    }

                    strm.Close();
                }
            }
            catch { }
            finally
            {
                rp.Input.Clear();
                rp.Output.Clear();
                rp.Phase = RemotingPhase.Disposing;
                handler(rp);
                rp.Session.Clear();
            }
        }

        /// <summary>
        /// Starts asynchronous client connection handling and may be interrupted later with Terminate()
        /// </summary>
        public void RunAsync()
        {
            if (AsyncRun != null) return;
            AsyncRun = new Thread(new ThreadStart(Run));
            AsyncRun.Start();
        }
        
        /// <summary>
        /// Dispose the server resources and terminates all active client connections.
        /// </summary>
        public void Dispose()
        {
            Terminate();
        }
    }
}
