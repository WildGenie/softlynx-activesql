using System;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SQLite;
using Softlynx.SQLiteDataset.Replication;


namespace Softlynx.SQLiteDataset.ActiveRecord
{
    public static  class Session
    {
        static private SQLiteConnection _Connection = null;
        static private string _FileName=string.Empty;
        static public SQLiteReplicator replica = new SQLiteReplicator();

        static public SQLiteConnection Connection
        {
            get { 
                if (_Connection==null) throw new Exception("Can't use uninitalized connection");
                return _Connection;
                }
        }

        static public string FileName
        {
            get
            {
                if (Connection!=null) return _FileName;
                return string.Empty;
            }
        }


        
        static public void AttachDatabase(string FileName)
        {
            Detach();
            _FileName = FileName;
            _Connection = new SQLiteConnection(string.Format("Data Source={0};BinaryGUID=true;", FileName));
            replica.MasterDB=Connection;
            replica.Open();
            RecordBase.InitStructure(System.Reflection.Assembly.GetCallingAssembly().GetTypes());
        }

        static public void RegisterAssemblyActiveRecordObjects()
        {
            if (Connection == null) return;
            RecordBase.InitStructure(System.Reflection.Assembly.GetCallingAssembly().GetTypes());
        }

        static public void Detach()
        {
            if (_Connection == null) return;
            replica.Ready = false;
            replica.MasterDB = null;
            RecordBase.ClearRegistrations();
            _Connection.Close();
            _Connection = null;
        }

        static public SQLiteCommand CreateCommand(string command,params object[] parameters)
        {
            SQLiteCommand cmd = new SQLiteCommand(command, Connection);
            int i = 0;
            while (i < parameters.Length)
            {
                string pname = parameters[i++].ToString();
                object pvalue = parameters[i++];
                cmd.Parameters.AddWithValue(pname, pvalue);
            }
            return cmd;
        }


        static public int RunCommand(string command, params object[] parameters)
        {
            using (SQLiteCommand cmd = CreateCommand(command, parameters))
            return cmd.ExecuteNonQuery();
        }

        static public object RunScalarCommand(string command, params object[] parameters)
        {
            using (SQLiteCommand cmd = CreateCommand(command, parameters))
                return cmd.ExecuteScalar();
        }

        static public SQLiteDataReader CreateReader(string command, params object[] parameters)
        {
            using (SQLiteCommand cmd = CreateCommand(command, parameters))
                return cmd.ExecuteReader();
        }


    }
}
