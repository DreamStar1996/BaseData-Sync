using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Text;

namespace DataSync
{
    class Program
    {
        static void Main(string[] args)
        {

            DataTable dtSource;
            DataTable dtTarget;
            DataTable dtResult;
            string ConnectionStringProgre = ConfigurationManager.AppSettings["ConnectionStringPostgre"];
            string ConnectionStringRaise = ConfigurationManager.AppSettings["ConnectionStringRaise"];
            string limitTime1 = ConfigurationManager.AppSettings["operationTime"];
            string limitTime2 = ConfigurationManager.AppSettings["room_status"];
            //room_status_log 表删除数据区间
            string[] limitTime3 = ConfigurationManager.AppSettings["room_status_delete"].Split(',');
            string sql = "";
            int ExecRet;
            
            try
            {
                Console.WriteLine("Sync Data Start");

                if (args[0] == "1")
                {
                    #region 同步 room_list表

                    dtSource = AdoContext.GetDataTable("select * from room_list", ConnectionStringProgre);
                    dtTarget = AdoContext.GetDataTableForSql("select * from room_list ", ConnectionStringRaise);
                    IEnumerable<DataRow> query = dtSource.AsEnumerable().Except(dtTarget.AsEnumerable(), DataRowComparer.Default);
                    if (query.Count() > 0)
                    {
                        //两个数据源的差集集合
                        dtResult = query.CopyToDataTable();
                        AdoContext.ImportDataSet(dtResult, ConnectionStringRaise, "room_list");
                    }
                    #endregion
                }

                if (args[0] == "2")
                {
                    #region 同步 operation_log 表
                    dtSource = AdoContext.GetDataTable(@"select * from operation_log where category = 'ROOM_CANCEL' and 
                                                update_time > now()::timestamp - interval '" + limitTime1 + " '", ConnectionStringProgre);
                    dtTarget = AdoContext.GetDataTableForSql("select * from operation_log ", ConnectionStringRaise);

                    IEnumerable<DataRow> query1 = dtSource.AsEnumerable().Except(dtTarget.AsEnumerable(), DataRowComparer.Default);
                    if (query1.Count() > 0)
                    {
                        //两个数据源的差集集合
                        dtResult = query1.CopyToDataTable();
                        AdoContext.ImportDataSet(dtResult, ConnectionStringRaise, "operation_log");
                    }

                    #endregion

                    #region 明细字段拆分
                    sql = @"
                        update operation_log
                        set subject = dbo.fnGetRoomHtmlVal(detail, 'subject:'),
                        startDate = dbo.fnGetRoomHtmlVal(detail, 'start:'),
                        endDate = dbo.fnGetRoomHtmlVal(detail, 'end'),
                        cancelDate = dbo.fnGetRoomHtmlVal(detail, 'cancelled time:')
                        where organizer != N'ROOM_ADMIN@SONY.COM' and(subject is null or subject = '')";

                    ExecRet = AdoContext.ExecSql(sql, ConnectionStringRaise);
                
                    if (ExecRet >= 0)
                    {
                        Console.WriteLine("Split Success!");
                    }
                    else
                    {
                        Console.WriteLine("Split fails!");
                    }
                    #endregion
                }

                if (args[0] == "3")
                {
                    #region 同步 room_status_log 表
                    sql = @" delete from room_status_log where ts >= dateadd(" + limitTime3[0] + "," + limitTime3[1] + ",getdate())";
                    ExecRet = AdoContext.ExecSql(sql, ConnectionStringRaise);
                    if (ExecRet < 0)
                    {
                        Console.WriteLine("delete fails!");
                        return;
                    }
                    
                    dtSource = AdoContext.GetDataTable("select * from room_status_log where ts > now()::timestamp - interval '" + limitTime1 + " '", ConnectionStringProgre);
                    dtTarget = AdoContext.GetDataTableForSql("select * from room_status_log ", ConnectionStringRaise);
                    IEnumerable<DataRow> query2 = dtSource.AsEnumerable().Except(dtTarget.AsEnumerable(), DataRowComparer.Default);
                    if (query2.Count() > 0)
                    {
                        //两个数据源的差集集合
                        dtResult = query2.CopyToDataTable();
                        AdoContext.ImportDataSet(dtResult, ConnectionStringRaise, "room_status_log");
                    }
                    #endregion
                }
                

                Console.WriteLine("Sync Data Success!");
            }
            
            catch (Exception ex)
            {
                Console.WriteLine("Error:" + ex.Message);
            }

        }
    }
}
