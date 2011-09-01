using System;
using System.IO;
using System.Collections.Generic;
using System.Text;
using SoftLynx.VDiffSharp;


namespace BSDiffTest
{
    public static class Patch
    {

        static void Main(string[] args)
        {
            VDiffEncoder.BzipEncode(
                        @"C:\temp\test\src",
                        @"C:\temp\test\dst",
                        @"C:\temp\test\patch1");

            VDiffDecoder.BzipDecode(
            @"C:\temp\test\src",
            @"C:\temp\test\patch1",
            @"C:\temp\test\dst.new");

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
             
    }
}
