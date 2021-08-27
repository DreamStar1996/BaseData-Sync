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

            DataTable dt;
            string source = ConfigurationManager.AppSettings["source"];
            string Target = ConfigurationManager.AppSettings["Target"];

            dt = AdoContext.GetDataTable("select * from 表名 limit 10", source);
            AdoContext.ImportDataSet(dt, Target, "room_list");

            dt = AdoContext.GetDataTable("select * from 表名 limit 10", source);
            AdoContext.ImportDataSet(dt, Target, "operation_log");
            
            dt = AdoContext.GetDataTable("select * from 表名 limit 10", source);
            AdoContext.ImportDataSet(dt, Target, "room_status_log");

        }
    }
}
