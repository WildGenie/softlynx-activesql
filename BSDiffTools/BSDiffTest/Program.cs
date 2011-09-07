using System;
using System.IO;
using System.Collections.Generic;
using System.Text;
using Softlynx.VDiffSharp;
using ICSharpCode.SharpZipLib.BZip2;
using zlib;
using SevenZip.Compression.LZMA;
using Softlynx.XDelta;


namespace BSDiffTest
{
    public class PipeStream : Stream
    {
        private Stream base_stream = null;
        public PipeStream(Stream BaseStream)
        {
            SwitchStream(BaseStream);
        }

        public void SwitchStream(Stream NewStream)
        {
            base_stream = NewStream;
        }
        public override bool CanRead
        {
            get { return base_stream.CanRead; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return base_stream.CanWrite; }
        }

        public override void Flush()
        {
            base_stream.Flush();
        }

        public override long Length
        {
            get { throw new Exception("The method or operation is not implemented."); }
        }

        public override long Position
        {
            get
            {
                return 0;
            }
            set
            {
                throw new Exception("The method or operation is not implemented.");
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return base_stream.Read(buffer, offset, count);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        public override void SetLength(long value)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            base_stream.Write(buffer, offset, count);
        }
    }
    public static class Patch
    {

        static void Main(string[] args)
        {
            Softlynx.XDelta.Wrapper.Encode(
                        @"C:\temp\test\src",
                        @"C:\temp\test\dst",
                        @"C:\temp\test\patch1");
            Softlynx.XDelta.Wrapper.Decode(
                        @"C:\temp\test\src",
                        @"C:\temp\test\patch1",
                        @"C:\temp\test\dst.new");


            return;

            Stream src=File.Open(@"C:\temp\test\src",FileMode.Open);
            Stream bzpart = File.Open(@"C:\temp\test\src.bz.1", FileMode.Create);
            //BZip2.Compress(src, bzpart, false, 9);
            
            PipeStream bzfile=new PipeStream(bzpart);
            Stream bzfilter = new ZOutputStream(bzfile,9);
            //StreamCopy(src, bzfilter);
            SevenZip.Compression.LZMA.Encoder encoder = new SevenZip.Compression.LZMA.Encoder();
            encoder.WriteCoderProperties(bzfile);
            encoder.Code(src, bzfile, 0, 0, null);
            //bzfilter.Flush();
            src.Close();
            bzpart.Close();
            src = File.Open(@"C:\temp\test\dst", FileMode.Open);
            bzpart = File.Open(@"C:\temp\test\src.bz.2", FileMode.Create);
            bzfile.SwitchStream(bzpart);
            encoder.Code(src, bzfile, 0, 0, null);
            //StreamCopy(src, bzfilter);
            bzfilter.Close();
            
            src.Close();
            bzpart.Close();




            
            /*
            VDiffEncoder.BzipEncode(
                        @"C:\temp\test\src",
                        @"C:\temp\test\dst",
                        @"C:\temp\test\patch1");

            VDiffDecoder.BzipDecode(
            @"C:\temp\test\src",
            @"C:\temp\test\patch1",
            @"C:\temp\test\dst.new");
            */
            return;
            /*
            Softlynx.BSDiffTools.Patch.Create(
               @"C:\temp\test\src",
               @"C:\temp\test\dst",
               @"C:\temp\test\patch");
            Softlynx.BSDiffTools.Patch.Apply(
               @"C:\temp\test\src",
               @"C:\temp\test\dst.newa",
               @"C:\temp\test\patch");
            SoftLynx.BSDiffSharp.BSPatch.Apply(
               @"C:\temp\test\src",
               @"C:\temp\test\dst.newb",
               @"C:\temp\test\patch");
                     */
        }

        private static void StreamCopy(Stream src, Stream dst)
        {
            byte[] buf = new byte[16384];
            int blksz = 0;
            do
            {
                blksz = src.Read(buf, 0, buf.Length);
                if (blksz > 0)
                    dst.Write(buf, 0, blksz);
                else break;
            } while (true);
        }
             
    }
}
