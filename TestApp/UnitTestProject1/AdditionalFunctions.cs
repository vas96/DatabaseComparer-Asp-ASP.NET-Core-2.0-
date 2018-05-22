using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;

namespace Comparer.Models
{
    public static class AdditionalFunctions
    {
        public static bool IsTypesComparable(string word1, string word2)
        {
            var Text_Family = new string[] {
                "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "BLOB", "MEDIUMTEXT",
                "MEDIUMBLOB", "LONGTEXT", "LONGBLOB", "ENUM", "SET", "NCHAR", "NVARCHAR", "NTEXT",
                "BINARY", "VARBINARY", "IMAGE"
            };
            var Numeric_Family = new string[] {"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL",
            "BIT", "BIGINT", "NUMERIC", "SMALLMONEY", "READ"
                };
            var Date_Family = new string[] { "DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR", "DATETIME2", "SMALLDATETIME", "DATETIMEOFFSET" };
            word1 = word1.ToUpper();
            word2 = word2.ToUpper();
            if ((Text_Family.Contains(word1) && (Text_Family.Contains(word2))) ||
                (Numeric_Family.Contains(word1) && (Numeric_Family.Contains(word2))) ||
                (Date_Family.Contains(word1) && (Date_Family.Contains(word2))))
                return true;
            return false;
        }


        public static int GetAvailablePort(int startingPort)
        {
            IPEndPoint[] endPoints;
            List<int> portArray = new List<int>();

            IPGlobalProperties properties = IPGlobalProperties.GetIPGlobalProperties();

            //getting active connections
            TcpConnectionInformation[] connections = properties.GetActiveTcpConnections();
            portArray.AddRange(from n in connections
                where n.LocalEndPoint.Port >= startingPort
                select n.LocalEndPoint.Port);

            //getting active tcp listners - WCF service listening in tcp
            endPoints = properties.GetActiveTcpListeners();
            portArray.AddRange(from n in endPoints
                where n.Port >= startingPort
                select n.Port);

            //getting active udp listeners
            endPoints = properties.GetActiveUdpListeners();
            portArray.AddRange(from n in endPoints
                where n.Port >= startingPort
                select n.Port);

            portArray.Sort();

            for (int i = startingPort; i < UInt16.MaxValue; i++)
                if (!portArray.Contains(i))
                    return i;

            return 0;
        }
    }
}
