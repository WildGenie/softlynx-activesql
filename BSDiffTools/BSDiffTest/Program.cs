using System;
using System.Collections.Generic;
using System.Text;
using Softlynx.BSDiffTools;

namespace BSDiffTest
{
    public static class Patch
    {

        static void Main(string[] args)
        {
            Softlynx.BSDiffTools.Patch.Create(
               @"C:\temp\snapshot-24bd9d47-0201-4a1f-a028-7be40f86c296-pc.db3",
               @"C:\temp\snapshot-2f7a30a8-be99-43b7-b00f-6e09381ea845-pc.db3",
               @"C:\temp\patch");
            Softlynx.BSDiffTools.Patch.Apply(
               @"C:\temp\snapshot-24bd9d47-0201-4a1f-a028-7be40f86c296-pc.db3",
               @"C:\temp\snapshot-2f7a30a8-be99-43b7-b00f-6e09381ea845-pc.db3-new",
    	       @"C:\temp\patch");
        }
    }
}
