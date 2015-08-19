<%@ WebHandler Language="C#" Class="HQSWcfService.ImportDataHandler" %>

using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data.OleDb;
using System.Data.Common;
using System.Data;
using System.Xml;
using System.IO;
using System.Collections;
using System.Net;
using System.Web.Compilation;
using System.ServiceModel;

namespace HQSWcfService
{
    /// <summary>
    /// DataImport 的摘要说明
    /// </summary>
    public class ImportDataHandler : IHttpHandler
    {
        private static bool DataImportLock = false;
        public void ProcessRequest(HttpContext context)
        {
            if (DataImportLock)
            {
                StaticClass.Write("正在同步。");
                context.Response.Write(false);
                context.Response.End();
                return;
            }
            DataImportLock = true;
            DateTime UpdateTime = new DateTime(2000, 1, 1);
	    DateTime UpdateTime1 = new DateTime(2000, 1, 1);

            try
            {
                #region 初始化本地数据

                bool clear = false;
 		bool clear1 = false;
		bool clear2 = false;

                UpdateTime =
                    //new DateTime(2000, 1, 1);
                     System.IO.File.Exists(AppDomain.CurrentDomain.BaseDirectory + "fredate") ?
                         DateTime.Parse(System.IO.File.ReadAllText(AppDomain.CurrentDomain.BaseDirectory + "fredate"))
                         : new DateTime(2000, 1, 1);
		UpdateTime1 =
                    //new DateTime(2000, 1, 1);
                     System.IO.File.Exists(AppDomain.CurrentDomain.BaseDirectory + "fredate") ?
                         DateTime.Parse(System.IO.File.ReadAllText(AppDomain.CurrentDomain.BaseDirectory + "fredate"))
                         : new DateTime(2000, 1, 1);
		if (DateTime.Now - UpdateTime.Date.AddHours(1) >= TimeSpan.FromDays(1))
                    clear = true;
		    
                if (clear)
                {
                    UpdateTime = DateTime.Now.Date;
                    System.IO.File.WriteAllText(AppDomain.CurrentDomain.BaseDirectory + "fredate",
                                            DateTime.Now.ToString("yyyy-MM-dd"), System.Text.Encoding.Default);

                    StaticClass.Write("清空患者队列表");
                    using (shinetriageEntities entities = new shinetriageEntities())
                    {
                        entities.ExecuteStoreCommand("delete from patient_queue");
                        entities.SaveChanges();
                    }
                }
		                
                if (DateTime.Now - UpdateTime1.AddHours(1) >= TimeSpan.FromHours(12))
                    clear1 = true;
		   
                if (clear1)
                {
                    UpdateTime1 = DateTime.Now.Hour>11? DateTime.Now.Date.AddHours(12):DateTime.Now.Date;
                    System.IO.File.WriteAllText(AppDomain.CurrentDomain.BaseDirectory + "fredate",
                                            UpdateTime1.ToString("yyyy-MM-dd HH:00:00"), System.Text.Encoding.Default);

                    StaticClass.Write("晚上和中午更新所有叫号器医生id");
                    using (shinetriageEntities entities = new shinetriageEntities())
                    {
                        entities.ExecuteStoreCommand("update pager set doctor_id = null");
                        entities.SaveChanges();
                    }
                }

		
                #endregion

                using (shinetriageEntities entities = new shinetriageEntities())
                {
                    foreach (var db_sourceData in entities.db_source.ToList())//数据源遍历
                    {
                        if (db_sourceData.dbType == true)//sql方式
                        {
                            StaticClass.Write("连接数据源" + db_sourceData.connectionstring);
                            OleDbDataAdapter oledbAdapter = new OleDbDataAdapter();
                            oledbAdapter.SelectCommand = new OleDbCommand();
                            oledbAdapter.SelectCommand.Connection = new OleDbConnection(db_sourceData.connectionstring);
                            foreach (var db_sqlData in db_sourceData.db_sql.Where(row => row.is_auto).OrderBy(row => row.type).ToList())
                            {
                                StaticClass.Write("-----------------------------------------------------------");
                                StaticClass.Write("导入数据:");
                                DateTime startTime = DateTime.Now;
                                int num = 0;
                                DataSetImportHIS dsHIS = new DataSetImportHIS();
                                oledbAdapter.SelectCommand.CommandText = db_sqlData.sqlstring;
                                switch (db_sqlData.type)
                                {
                                    case 1:
                                        #region 导入Doctor
                                        try
                                        {
                                            oledbAdapter.Fill(dsHIS.doctor);
                                        }
                                        catch (Exception ex)
                                        {
                                            StaticClass.Write("读取Doctor时出现错误：" + ex.Message.ToString());
                                            StaticClass.Write("SQL语句：" + db_sqlData.sqlstring);
                                        }

                                        foreach (var doctorHISData in from doctorHIS in dsHIS.doctor
                                                                      join doctorData in entities.doctor
                                                                      on doctorHIS.login_id equals doctorData.login_id into temp
                                                                      from t in temp.DefaultIfEmpty()
                                                                      where t == null
                                                                      select doctorHIS
                                            )
                                        {
                                            num++;
                                            var doctorData = doctor.Createdoctor(
                                                    0,
                                                    doctorHISData.login_id,
                                                    db_sqlData.db_source_id,
                                                    doctorHISData.name,
                                                    doctorHISData.IsdepartmentNull() ? "" : doctorHISData.department,
                                                    doctorHISData.IstitleNull() ? "" : doctorHISData.title);
                                            doctorData.db_source_id = db_sourceData.id;
                                            entities.doctor.AddObject(doctorData);
                                        }
                                        StaticClass.Write("导入doctor数据:" + num.ToString() + "条");
                                        #endregion
                                        break;
                                    case 2:
                                        #region 导入QueueType
                                        try
                                        {
                                            oledbAdapter.Fill(dsHIS.queue_type);
                                        }
                                        catch (Exception ex)
                                        {
                                            StaticClass.Write("读取QueueType时出现错误：" + ex.Message.ToString());
                                            StaticClass.Write("SQL语句：" + db_sqlData.sqlstring);
                                        }

                                        foreach (var queueTypeHISData in from queue_typeHIS in dsHIS.queue_type
                                                                         join queueTypeData in entities.queue_type
                                                                         on queue_typeHIS.source_id equals queueTypeData.source_id into temp
                                                                         from t in temp.DefaultIfEmpty()
                                                                         where t == null
                                                                         select queue_typeHIS)
                                        {
                                            num++;
                                            var queueTypeData = queue_type.Createqueue_type(
                                                    0,
                                                    queueTypeHISData.source_id,
                                                    "",
                                                    queueTypeHISData.is_reorder,
                                                    queueTypeHISData.is_checkin,
                                                    queueTypeHISData.is_pretriage,
                                                    0,
                                                    queueTypeHISData.name);
                                            queueTypeData.displayname = queueTypeHISData.displayname;
                                            queueTypeData.db_source_id = db_sourceData.id;
                                            entities.queue_type.AddObject(queueTypeData);
                                        }
                                        StaticClass.Write("导入QueueType数据:" + num.ToString() + "条");
                                        #endregion
                                        break;
                                    case 4:
                                        #region 导入rlt_doctor2queue_type
                                        try
                                        {
                                            oledbAdapter.Fill(dsHIS.rlt_doctor2queue_type);
                                            // StaticClass.dtDecoding(dsHIS.rlt_doctor2queue_type, SourceRow.description);
                                        }
                                        catch (Exception ex)
                                        {
                                            StaticClass.Write("读取rlt_doctor2queue_type时出现错误：" + ex.Message.ToString());
                                            StaticClass.Write("SQL语句：" + db_sqlData.sqlstring);
                                        }

                                        foreach (var rltDoctorData in from rltHis in
                                                                          (from rltHIS in dsHIS.rlt_doctor2queue_type
                                                                           join rltHQS in entities.rlt_doctor2queue_type
                                                                           on new { a = rltHIS.login_id, b = rltHIS.queue_type_source_id }
                                                                               equals
                                                                               new
                                                                               {
                                                                                   a = rltHQS == null ? null : rltHQS.doctor.login_id,
                                                                                   b = rltHQS == null ? null : rltHQS.queue_type.source_id
                                                                               } into temp
                                                                           from t in temp.DefaultIfEmpty()
                                                                           where t == null
                                                                           select rltHIS)
                                                                      join doctorData in entities.doctor on rltHis.login_id equals doctorData.login_id
                                                                      join queueTypeData in entities.queue_type on rltHis.queue_type_source_id equals queueTypeData.source_id
                                                                      select new { rltHis, doctorData.doctor_id, queueTypeData.queue_type_id }


                                            )
                                        {
                                            num++;
                                            var doctor2queue_type = rlt_doctor2queue_type.Createrlt_doctor2queue_type(
                                                    0,
                                                    rltDoctorData.doctor_id,
                                                    rltDoctorData.queue_type_id,
                                                    rltDoctorData.rltHis.onduty,
                                                    "1,1,1,1,1,1,1",
                                                    "0000000",
                                                    false);
                                            entities.rlt_doctor2queue_type.AddObject(doctor2queue_type);
                                        }

                                        StaticClass.Write("导入rlt_doctor2queue_type数据:" + num.ToString() + "条");
                                        #endregion
                                        break;
                                    case 5:
                                        #region 导入PatientQueue
                                        int? tmpDbSourceId = db_sqlData.db_source_id;
                                        var queueTypeList = (from queueTypeData in entities.queue_type
                                                             where queueTypeData.db_source_id == tmpDbSourceId//db_sqlData.db_source_id
                                                             && !queueTypeData.is_checkin
                                                             select queueTypeData).ToList();
                                        var queueTypeSourceId = queueTypeList.Select(p => p.source_id).ToList();
                                        string filtersql = ReplaceSql(db_sqlData.sqlstring, "queue_type_source_id in ('" + string.Join("','", queueTypeSourceId) + "')");
                                        try
                                        {
                                            oledbAdapter.SelectCommand.CommandText = filtersql;
                                            dsHIS.patient_queue.Clear();
                                            oledbAdapter.Fill(dsHIS.patient_queue);
                                        }
                                        catch (Exception ex)
                                        {
                                            StaticClass.Write("读取PatientQueue时出现错误：" + ex.Message.ToString());
                                            StaticClass.Write("SQL语句：" + db_sqlData.sqlstring);
                                            if (ex.InnerException != null)
                                            {
                                                StaticClass.Write("<br>InnerException：" + ex.InnerException.Message);
                                                StaticClass.Write(ex.InnerException.StackTrace);
                                            }
                                        }

                                        foreach (var patientQueueHISData in from patientRowHIS in dsHIS.patient_queue.Where(p => queueTypeList.Any(q => q.source_id == p.queue_type_source_id))
                                                                            join patientQueueData in entities.patient_queue
                                                                                on new { a = patientRowHIS.source_id, b = tmpDbSourceId } equals
                                                                                new { a = patientQueueData.source_id, b = patientQueueData.db_source_id } into temp
                                                                            from t in temp.DefaultIfEmpty()
                                                                            where t == null
                                                                            select patientRowHIS
                                            )
                                        {
                                            double queue_num = 0;
                                            queue_type queueTypeData = queueTypeList.First(q => q.source_id == patientQueueHISData.queue_type_source_id);
                                            if (queueTypeData.is_reorder && !queueTypeData.is_pretriage)
                                            {
                                                queue_num = queueTypeData.patient_queue.Count() > 0 ? queueTypeData.patient_queue.Max(p => p.queue_num) + 1 : 1;
                                                queue_num = queueTypeData.reserve_numlist.Length > 0 ? GetQueueNum(queue_num, queueTypeData) : queue_num;
                                            }
                                            var patientQueue = patient_queue.Createpatient_queue(
                                                   0,
                                                   patientQueueHISData.patient_name.Length > 10 ? patientQueueHISData.patient_name.Substring(0,10):patientQueueHISData.patient_name,
                                                   queueTypeData.queue_type_id,
                                                   queueTypeData.is_reorder ? queue_num.ToString() : patientQueueHISData.register_id,
                                                   queueTypeData.is_reorder ? queue_num : patientQueueHISData.queue_num,
                                                   patientQueueHISData.sub_queue_order,
                                                   patientQueueHISData.Issub_queue_typeNull() ? "" : patientQueueHISData.sub_queue_type,
                                                   false,
                                                   false,
                                                   0,
                                                   0,
                                                   false,
                                                   queueTypeData.is_reorder ? (byte)0 : patientQueueHISData.time_interval,
                                                   patientQueueHISData.fre_date,
                                                   DateTime.MinValue);
                                            patientQueue.db_source_id = db_sqlData.db_source_id;
                                            patientQueue.source_code = patientQueueHISData.source_code;
                                            patientQueue.source_id = patientQueueHISData.source_id;
                                            patientQueue.source_code = patientQueueHISData.source_code;
                                            patientQueue.content = patientQueueHISData.IscontentNull() ? "" : patientQueueHISData.content;
                                            if (!patientQueueHISData.Isdoctor_source_idNull())
                                            {
                                                var doctorData = entities.doctor.FirstOrDefault(d => d.login_id == patientQueueHISData.doctor_source_id);
                                                if (doctorData != null)
                                                    patientQueue.doctor_id = doctorData.doctor_id;
                                            }
                                            entities.patient_queue.AddObject(patientQueue);

                                            num++;
                                        }
                                        #endregion
                                        StaticClass.Write("导入PatientQueue数据:" + num.ToString() + "条");
                                        break;
                                    case 6:
                                        #region 导入Patient_Queue_Item
                                        try
                                        {
                                            oledbAdapter.SelectCommand.CommandText = db_sqlData.sqlstring;
                                            dsHIS.patient_queue_item.Clear();
                                            oledbAdapter.Fill(dsHIS.patient_queue_item);
                                        }
                                        catch (Exception ex)
                                        {
                                            StaticClass.Write("读取PatientQueue时出现错误：" + ex.Message.ToString());
                                            StaticClass.Write("SQL语句：" + db_sqlData.sqlstring);
                                            if (ex.InnerException != null)
                                            {
                                                StaticClass.Write("InnerException：" + ex.InnerException.Message);
                                                StaticClass.Write(ex.InnerException.StackTrace);
                                            }
                                        }

                                        var patientQueueList = entities.patient_queue.ToList();
                                        foreach (var patientItemData in from patientItemRowHIS in dsHIS.patient_queue_item
                                                                        join patientQueueItemData in
                                                                            (from item in entities.patient_queue_item
                                                                             select new { item, source_id = item.patient_queue.source_id })
                                                                        on new { a = patientItemRowHIS.source_id, b = patientItemRowHIS.check_name } equals
                                                                            new { a = patientQueueItemData.source_id, b = patientQueueItemData.item.check_name } into temp
                                                                        from t in temp.DefaultIfEmpty()
                                                                        where t == null
                                                                        select patientItemRowHIS
                                                                        )
                                        {
                                            var patientQueueData = patientQueueList.FirstOrDefault(p => p.source_id == patientItemData.source_id && p.queue_type.source_id == patientItemData.queue_type_source_id);
                                            if (patientQueueData != null)
                                            {
                                                var patientQueueItem = patient_queue_item.Createpatient_queue_item(0, patientQueueData.id, false);
                                                patientQueueItem.db_source_id = db_sqlData.db_source_id;
                                                patientQueueItem.check_name = patientItemData.check_name;
                                                entities.patient_queue_item.AddObject(patientQueueItem);
                                                num++;
                                            }
                                        }
                                        StaticClass.Write("导入PatientQueueItem数据:" + num.ToString() + "条");

                                        #endregion
                                        break;
                                }
                                entities.SaveChanges();
                                dsHIS.Dispose();
                                StaticClass.Write("耗时: " + (DateTime.Now - startTime).TotalSeconds.ToString() + " 秒");
                            }
                        }
                        else//web
                        {
                        }
                    }
                }
                DataImportLock = false;
                context.Response.Write(true);
                //context.Response.End();
                return;
            }
            catch (Exception ex)
            {
                StaticClass.Write("错误信息如下:");
                StaticClass.Write(ex.Message.ToString());
                StaticClass.Write(ex.StackTrace.ToString());
                if (ex.InnerException != null)
                {
                    StaticClass.Write("InnerException:");
                    StaticClass.Write(ex.InnerException.Message);
                    StaticClass.Write(ex.InnerException.StackTrace);
                }

                DataImportLock = false;
                context.Response.Write(false);
                context.Response.End();
                return;
            }
        }

        public bool IsReusable
        {
            get
            {
                return false;
            }
        }
        private double GetQueueNum(double queue_num, queue_type queueType)
        {
            double queueNum = queue_num;
            StaticClass.Write(queue_num.ToString() + ",##");
            if (queueType.reserve_numlist.Split(new char[] { ',' }).Contains(queue_num.ToString()))
            {
                queue_num += 1;
                queueNum = GetQueueNum(queue_num, queueType);
            }

            return queueNum;
        }
        private string ReplaceSql(string sql, string filter)
        {
            sql = sql.ToUpper();

            bool flagWhere = false;
            if (sql.Contains("WHERE"))
            {
                string strLast = sql.Substring(sql.LastIndexOf("WHERE"));
                if (strLast.Count(s => s == '(') == strLast.Count(s => s == ')'))
                {
                    flagWhere = true;
                }
            }
            if (flagWhere)
                sql = sql.Substring(0, sql.LastIndexOf("WHERE")) + "where " + filter + " and" + sql.Substring(sql.LastIndexOf("WHERE") + 5);
            else if (sql.Contains("ORDER BY"))
                sql = sql.Replace("ORDER BY", "where " + filter + " order by");
            else
                sql = sql + " where " + filter;


            return sql;
        }
    }
}
