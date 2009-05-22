using System;
using System.Collections.Generic;
using System.Collections;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Runtime.InteropServices;

namespace Softlynx.ActiveSQL.OleDB
{
    public struct Money:IConvertible  
    {
        private decimal _value;


        public Money(Decimal v)
        {
            _value = v;
        }

        public Money(Money m)
        {
            _value = m._value;
        }

        public static implicit operator Decimal(Money m)
        {
            return m._value;
        }

        public static implicit operator Money(Decimal v)
        {
            return new Money(v);
        }

        public static implicit operator Money(double v)
        {
            return new Money((decimal)v);
        }

        public static implicit operator Money(int v)
        {
            return new Money((decimal)v);
        }

        public override string ToString()
        {
            return _value.ToString();
        }

        #region IConvertible Members

        public TypeCode GetTypeCode() { return TypeCode.Decimal; }

        public bool ToBoolean(IFormatProvider provider) { return _value != decimal.Zero; }


        public byte ToByte(IFormatProvider provider)
        {
            return (byte)_value;
        }

        public char ToChar(IFormatProvider provider)
        {
            return (char)_value;
        }

        public DateTime ToDateTime(IFormatProvider provider)
        {
            return DateTime.MinValue+TimeSpan.FromSeconds((double)_value);
        }

        public decimal ToDecimal(IFormatProvider provider)
        {
            return _value;
        }

        public double ToDouble(IFormatProvider provider)
        {
            return (double)_value;
        }

        public short ToInt16(IFormatProvider provider)
        {
            return (Int16)_value;
        }

        public int ToInt32(IFormatProvider provider)
        {
            return (Int32)_value;
        }

        public long ToInt64(IFormatProvider provider)
        {
            return (Int64)_value;
        }

        public sbyte ToSByte(IFormatProvider provider)
        {
            return (sbyte)_value;
        }

        public float ToSingle(IFormatProvider provider)
        {
            return (float)_value;
        }

        public string ToString(IFormatProvider provider)
        {
            return _value.ToString();
        }

        public object ToType(Type conversionType, IFormatProvider provider)
        {
            return Convert.ChangeType(_value,conversionType,provider);
        }

        public ushort ToUInt16(IFormatProvider provider)
        {
            return (UInt16)_value;
        }

        public uint ToUInt32(IFormatProvider provider)
        {
            return (UInt32)_value;
        }

        public ulong ToUInt64(IFormatProvider provider)
        {
            return (UInt64)_value;
        }

        #endregion

        #region Money<->Money operators
        public static Money operator +(Money v1, Money v2)
        {
            return new Money(v1._value + v2._value);
        }

        public static Money operator -(Money v1, Money v2)
        {
            return new Money(v1._value - v2._value);
        }

        public static Money operator *(Money v1, Money v2)
        {
            return new Money(v1._value * v2._value);
        }

        public static Money operator /(Money v1, Money v2)
        {
            return new Money(v1._value / v2._value);
        }
        #endregion

      
    };
    public class OleDBSpecifics : IProviderSpecifics
    {
        OleDbConnectionStringBuilder sb = new OleDbConnectionStringBuilder();
        DbConnection db = new OleDbConnection();

        private static Hashtable CreateTypeMapping()
        {
        // http://msdn.microsoft.com/en-us/library/bb208866.aspx
            Hashtable res = new Hashtable();
            res[typeof(string)] = new object[] { "TEXT", DbType.String };
            res[typeof(Int16)] = new object[] { "SMALLINT", DbType.Int16 };
            res[typeof(Int32)] = new object[] { "INTEGER", DbType.Int32 };
            res[typeof(Int64)] = new object[] { "MONEY", DbType.Int64 };
            res[typeof(DateTime)] = new object[] { "DATETIME", DbType.DateTime };
            res[typeof(decimal)] = new object[] { "DECIMAL", DbType.Decimal };
            res[typeof(Money)] = new object[] { "MONEY", DbType.Currency};
            res[typeof(double)] = new object[] { "FLOAT", DbType.Double };
            res[typeof(bool)] = new object[] { "BIT", DbType.Boolean };
            res[typeof(Guid)] = new object[] { "UNIQUEIDENTIFIER", DbType.Guid };
            res[typeof(Object)] = new object[] { "BINARY", DbType.Binary };
            res[typeof(byte[])] = new object[] { "BINARY", DbType.Binary };
            return res;
        }

        Hashtable type_mapping = CreateTypeMapping();

        public DbParameter CreateParameter(string name, object value)
        {
            DbParameter p = new OleDbParameter();
            p.DbType = GetDbType(value.GetType());
            p.ParameterName = name;
            p.Value = value;
            return p;
        }

        public DbParameter CreateParameter(string name, Type type)
        {
            DbParameter p = new OleDbParameter();
            p.DbType = GetDbType(type);
            p.ParameterName = name;
            return p;
        }

        public DbParameter SetupParameter(DbParameter param, InField f)
        {
            OleDbParameter odbp=(OleDbParameter)param;
            odbp.Size = f.Size;
            if (odbp.Size == 0)
            {
                switch (GetDbType(f.FieldType))
                {
                    case DbType.String:
                        odbp.Size = 1024;
                        break;
                    default:
                        odbp.Size =  Marshal.SizeOf(f.FieldType);
                        break;
                }
            }
            odbp.Scale = f.Scale;
            odbp.Precision = f.Precision;
            return odbp;
        }

        public string GetSqlType(Type t)
        {
            throw new NotImplementedException();
            object[] o = (object[])type_mapping[t];
            if (t.IsEnum)
                o = (object[])type_mapping[typeof(int)];
            if (o == null) return "BINARY";
            return (string)o[0];
        }


        public DbType GetDbType(Type t)
        {
            object[] o = (object[])type_mapping[t];
            if (t.IsEnum)
                o = (object[])type_mapping[typeof(int)];
            if (o == null) return DbType.Object;
            return (DbType)o[1];
        }


        public string AsFieldName(string s)
        {
            return string.Format("{0}", s);
        }

        public string AsFieldParam(string s)
        {
            return string.Format("@{0}", s);
        }

        public string AutoincrementStatement(string ColumnName)
        {
            throw new NotImplementedException();
            //return string.Format("{0} BIGSERIAL", AsFieldName(ColumnName));
        }

        public DbConnection Connection
        {
            get
            {
                return db;
            }
        }

        public void ExtendConnectionString(string key, string value)
        {
            sb.Add(key, value);
            db.ConnectionString = sb.ConnectionString;

        }

        public string AdoptSelectCommand(string select, InField[] fields)
        {
            return select;
        }
    }
}
