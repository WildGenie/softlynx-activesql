using System;
using System.Collections.Generic;
using System.Text;
using BSDiffTools;

namespace BSDiffTest
{
    public static class Patch
    {

        static void Main(string[] args)
        {
            BSDiffTools.Patch.Create(
               @"C:\temp\snapshot-f384bab0-328c-40c8-8ea7-adf571146189.db3",
               @"C:\temp\snapshot-fec0afa7-fcc1-4788-9fdd-0cd8f6ad4c20.db3",
               @"C:\temp\patch");
            BSDiffTools.Patch.Apply(
               @"C:\temp\snapshot-f384bab0-328c-40c8-8ea7-adf571146189.db3",
	           @"C:\temp\snapshot-fec0afa7-fcc1-4788-9fdd-0cd8f6ad4c20.db3-new",
    	       @"C:\temp\patch");
        }
    }
}
