using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace DataSync
{
    public class AdoContext
    {
        /// <summary>
        /// 执行sql返回DataTable
        /// </summary>
        /// <param name="Sql"></param>
        /// <param name="strConnection"></param>
        /// <returns></returns>
        public static DataTable GetDataTable(string Sql, string strConnection)
        {
            NpgsqlConnection SqlConn = new NpgsqlConnection(strConnection);
            DataSet ds = new DataSet();
            DataTable dt;
            try
            {
                using (NpgsqlDataAdapter sqldap = new NpgsqlDataAdapter(Sql, SqlConn))
                {
                    sqldap.Fill(ds);
                    dt = ds.Tables[0];
                }
                return dt;
            }
            catch (System.Exception ex)
            {
                return null;
            }

        }





        /// <summary>
        /// 执行sql返回DataTable
        /// </summary>
        /// <param name="Sql"></param>
        /// <param name="strConnection"></param>
        /// <returns></returns>
        public static void ImportDataSet(DataTable DestDataTable, string strConnection,string tableName)
        {
            try
            {
                SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(strConnection, SqlBulkCopyOptions.Default);
                sqlBulkCopy.BulkCopyTimeout = 6000;
                sqlBulkCopy.DestinationTableName = tableName;

                foreach (DataColumn dataColumn in DestDataTable.Columns)
                {

                    sqlBulkCopy.ColumnMappings.Add(dataColumn.ColumnName, dataColumn.ColumnName);
                }
                
                sqlBulkCopy.WriteToServer(DestDataTable);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }







    }
}
