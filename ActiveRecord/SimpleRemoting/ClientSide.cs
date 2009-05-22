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
    /// Base class for successors remoting exceptions
    /// </summary>
    public abstract class RemotingException : ApplicationException {
        /// <summary>
        /// Instantiate an excaption with specified message text
        /// </summary>
        /// <param name="message">Exception text message</param>
        public RemotingException(string message):base(message) { }
    }
    
    /// <summary>
    /// Raise in case of improper communication protocol flow and actually should never be happens.
    /// </summary>
    public class RemotingProtocolException : RemotingException
    {
        /// <summary>
        /// Instantiate an excaption with specified message text
        /// </summary>
        /// <param name="message">Exception text message</param>
        public RemotingProtocolException(string message) : base(message) { }
    }

    /// <summary>
    /// Raise in case of handler exception on remote side
    /// </summary>
    public class RemotingHandlerException : RemotingException
    {
        /// <summary>
        /// Instantiate an excaption with specified message text
        /// </summary>
        /// <param name="message">Exception text message</param>
        public RemotingHandlerException(string message) : base(message) { }
    }

    /// <summary>
    /// Client side remoting component in form of TCP connection handles queries to server and back replies.
    /// <example>
    /// <code>
    /// using Softlynx.SimpleRemoting;
    /// ....
    /// using (Client cli = new Client(new IPEndPoint(IPAddress.Parse("target.server.net"), 9090)))
    /// {
    /// RemotingParams p = new RemotingParams();
    /// p.Input.Add("ParamKey1","ParamValue1");
    /// try {
    /// cli.Query(p);
    /// } catch {...}
    /// //examine p.Output
    /// if (p.Output["ReplyKey1"]=="ReplyValue") 
    /// {...}
    /// }
    /// </code>
    /// </example>
    /// </summary>
    public class Client:IDisposable
    {
        private TcpClient connection = null;
        private StreamReader rd = null;
        private StreamWriter wr = null;

        private string ResponseBlock(string size,string response)
        {
            int blocklen = 0;
            try
            {
                blocklen = int.Parse(size);
            }
            catch            
            {
                throw new RemotingProtocolException("Unexpected remoting server response " + response);
            }
            if (blocklen > Server.MAX_PARAM_LENGTH)
                throw new RemotingProtocolException("Oversized parameter: " + response);
            char[] buf = new char[blocklen];
            rd.ReadBlock(buf, 0, blocklen);
            return new string(buf);
        }

        /// <summary>
        /// Prepare and establish a new connection to remote server
        /// </summary>
        /// <param name="server">TCP IP endpoint (address and port)</param>
        public Client(IPEndPoint server)
        {
            connection = new TcpClient();
            connection.Connect(server);
            NetworkStream s=connection.GetStream();
            rd = new StreamReader(s);
            wr = new StreamWriter(s);
            string response = rd.ReadLine();
            string []hello = response.Split(new char[]{' '});
            if (
                (hello.Length < 3)
                ||
                (hello[0] != "200")
                ||
                (hello[1] != "READY")
                ||
                (hello[2] != Server.VERSION)
                ) throw new RemotingProtocolException("Unexpected remoting server response " + response);
        }
        
        /// <summary>
        /// Pass the query across the network to the server side and get the responce back.
        /// In case of an remote sede exception inside MessageHandler delegate 
        /// it will be raised localy as RemotingHandlerException
        /// </summary>
        /// <param name="parameters">Hold input key value pairs and gets output up on return</param>
        public void Query(RemotingParams parameters)
        {
            foreach (KeyValuePair<string, string> kvp in parameters.Input)
            {
                wr.WriteLine("{0} {1}", kvp.Key.Replace('\r', ' ').Replace('\n', ' '), kvp.Value.Length);
                wr.Write(kvp.Value);
                wr.Flush();
                string confirm = rd.ReadLine();
                if (confirm != "202 OK")
                    throw new RemotingProtocolException("Unexpected remoting server response " + confirm);
            }
            wr.WriteLine("EXEC");
            wr.Flush();
                string response = rd.ReadLine();
                string[] result = response.Split(new char[] { ' ' });
                if (result.Length == 3)
                {
                    if ((result[0] == "500") && (result[1] == "EXCEPTION"))
                    {
                        throw new RemotingHandlerException(ResponseBlock(result[2],response));
                    }
                    if ((result[0] == "200") && (result[1] == "OK"))
                    {
                        parameters.Output.Clear();
                        int count = 0;
                        try
                        {
                            count = int.Parse(result[2]);
                            for (int i = 0; i < count; i++)
                            {
                                response = rd.ReadLine();
                                int sep = response.LastIndexOf(' ');
                                string field = sep < 0 ? null : response.Substring(0, sep);
                                string value = response.Substring(sep + 1);
                                parameters.Output[field] = ResponseBlock(value, response);
                            }
                            return;
                        }
                        catch { }
                    }
                }
            throw new RemotingProtocolException("Unexpected remoting server response " + response);
        }

        /// <summary>
        /// Release all the communication resources
        /// </summary>
        public void Dispose()
        {
            connection.Close();
        }
    }
}
