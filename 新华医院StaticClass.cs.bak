using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data;

namespace HQSWcfService
{
    public static class StaticClass
    {
        /// <summary>
        /// 写的方法，将下载的日志写入本地文件
        /// </summary>
        /// <param name="content">传递要写的内容</param>
        public static void Write(string content)
        {
            if (System.Configuration.ConfigurationManager.AppSettings["WriteLog"] == "1")
            {
                //string folderPath = AppDomain.CurrentDomain.BaseDirectory + "\\Log";
                string folderPath = "D:\\Log";
                if (!Directory.Exists(folderPath))
                {
                    Directory.CreateDirectory(folderPath);
                }
                string filePath = folderPath + "\\" + DateTime.Now.ToString("D") + "_log.txt";

                foreach (string item in Directory.GetFiles(folderPath))
                {
                    DateTime dayBeforeMonth = Convert.ToDateTime(File.GetCreationTime(item).ToShortDateString());
                    if ((dayBeforeMonth - DateTime.Now).TotalDays > 30)
                    {
                        File.Delete(item);
                    }
                }

                File.AppendAllText(filePath, DateTime.Now.ToString("yyyy/MM/dd hh:mm:ss ffffff") + ":" + content + "\r\n");
            }
        }

        public static IEnumerable<patient_queue> PatientWaitingList(HQSWcfService.shinetriageEntities entities, queue_type queue_typeData, int Count)
        {
            var triageData = queue_typeData.triage;
            int dayOfWeek = (int)DateTime.Now.DayOfWeek;
            var queueTypeEnable = (
                                        triageData.triage_type == 1 && queue_typeData.rlt_doctor2queue_type.Count(rlt => rlt.queue_type.triage_id == triageData.triage_id
                                                                                        && rlt.onduty[dayOfWeek] != '0'
                                                                                        && (DateTime.Now.Hour < 12 ? rlt.onduty[dayOfWeek] != '3' :
                                                                                                                    rlt.onduty[dayOfWeek] != '2')) == 1)
                                        ||
                                        (triageData.triage_type == 2 && queue_typeData.rlt_pager2queue_type.Count(rlt => rlt.queue_type.triage_id == triageData.triage_id
                                                                                        && rlt.onduty[dayOfWeek] != '0'
                                                                                        && (DateTime.Now.Hour < 12 ? rlt.onduty[dayOfWeek] != '3' :
                                                                                                                    rlt.onduty[dayOfWeek] != '2')) == 1);
            var patientQueueList = from patientQueueData in entities.patient_queue
                                   where !patientQueueData.is_call
                                        && patientQueueData.queue_type_id == queue_typeData.queue_type_id
                                        && patientQueueData.state_triage != 5
                                        && (queueTypeEnable || (!patientQueueData.call_time.HasValue && patientQueueData.state_triage != 1))
                                   orderby
                                         patientQueueData.time_interval,
                                         patientQueueData.is_priority descending,
                                         patientQueueData.sub_queue_order descending,
                                         patientQueueData.queue_num,
                                         patientQueueData.fre_date
                                   select patientQueueData;
            var listPriority = (from p in patientQueueList
                                where p.is_priority
                                orderby p.opr_time
                                select p
                        ).Take(Count).ToList();
            if (listPriority.Count() == Count)
                return listPriority;

            var listFirst = (from p in patientQueueList
                             where !p.is_priority && p.state_call == 0 && p.state_triage == 0
                             select p).Take(Count).ToList();

            var listPass = (from p in patientQueueList
                            where !p.is_priority && p.state_call == 1
                            orderby p.opr_time
                            select p).Take(Count).ToList();

            var listReturn = (from p in patientQueueList
                              where !p.is_call && !p.is_priority && p.state_triage == 1 && p.state_call != 1
                              orderby p.opr_time
                              select p).Take(Count).ToList();

            var tmpFirst = from p in listFirst
                           select new
                           {
                               data = p,
                               index = listFirst.IndexOf(p)
                           };

            var tmpPass = from p in listPass
                          select new
                          {
                              data = p,
                              index = listPass.Take(listPass.IndexOf(p) + 1).Sum(t => t.return_flag).Value
                          };

            var listFirstAndPass = (from p in tmpFirst.Union(tmpPass)
                                    orderby p.index, p.data.state_call descending
                                    select p.data).ToList();

            var rtn = listPriority.Union(
                                        from patient in
                                            (
                                                (from p in listFirstAndPass
                                                 select new
                                                 {
                                                     data = p,
                                                     index = listFirstAndPass.IndexOf(p)
                                                 }).Union(
                                                from p in listReturn
                                                select new
                                                {
                                                    data = p,
                                                    index = listReturn.Take(listReturn.IndexOf(p) + 1).Sum(t => t.return_flag).Value
                                                }
                                                )
                                                )
                                        orderby patient.index, patient.data.state_triage descending
                                        select patient.data).Take(Count);

            return rtn;
        }

        public static IEnumerable<patient_queue> PatientWaitingList(HQSWcfService.shinetriageEntities entities, pager pagerData, int Count)
        {
            var triageData = pagerData.triage;
            int dayOfWeek = (int)DateTime.Now.DayOfWeek;

            var queueTypeEnable = (
                                    triageData.triage_type == 1 && pagerData.doctor.rlt_doctor2queue_type.Count(rlt => rlt.queue_type.triage_id == triageData.triage_id
                                                                                    && rlt.onduty[dayOfWeek] != '0'
                                                                                    && (DateTime.Now.Hour < 12 ? rlt.onduty[dayOfWeek] != '3' :
                                                                                                                rlt.onduty[dayOfWeek] != '2')) == 1)
                                    ||
                                    (triageData.triage_type == 2 && pagerData.rlt_pager2queue_type.Count(rlt => rlt.queue_type.triage_id == triageData.triage_id
                                                                                    && rlt.onduty[dayOfWeek] != '0'
                                                                                    && (DateTime.Now.Hour < 12 ? rlt.onduty[dayOfWeek] != '3' :
                                                                                                                rlt.onduty[dayOfWeek] != '2')) == 1);

            var queue_type = queueTypeEnable ? (triageData.triage_type == 1 ? pagerData.doctor.rlt_doctor2queue_type.FirstOrDefault().queue_type : pagerData.rlt_pager2queue_type.FirstOrDefault().queue_type) : null;
            if (queue_type != null)
            {
                queueTypeEnable = (triageData.triage_type == 1 && queue_type.rlt_doctor2queue_type.Count(rlt => rlt.queue_type.triage_id == triageData.triage_id
                                                                                    && rlt.onduty[dayOfWeek] != '0'
                                                                                    && (DateTime.Now.Hour < 12 ? rlt.onduty[dayOfWeek] != '3' :
                                                                                                                rlt.onduty[dayOfWeek] != '2')) == 1)
                                    ||
                                    (triageData.triage_type == 2 && queue_type.rlt_pager2queue_type.Count(rlt => rlt.queue_type.triage_id == triageData.triage_id
                                                                                    && rlt.onduty[dayOfWeek] != '0'
                                                                                    && (DateTime.Now.Hour < 12 ? rlt.onduty[dayOfWeek] != '3' :
                                                                                                                rlt.onduty[dayOfWeek] != '2')) == 1);
            }

            var queue_type_id = queueTypeEnable ? (triageData.triage_type == 1 ? pagerData.doctor.rlt_doctor2queue_type.FirstOrDefault().queue_type_id : pagerData.rlt_pager2queue_type.FirstOrDefault().queue_type_id) : 0;

            var patientQueueList = from patientQueueData in entities.patient_queue
                                   where !patientQueueData.is_call
                                        && patientQueueData.queue_type.is_pretriage ? (
                                                                                        (triageData.triage_type == 1 && patientQueueData.doctor_id == pagerData.doctor_id)
                                                                                        || (triageData.triage_type == 2 && patientQueueData.pager_id == pagerData.pager_id))
                                                                                    : (
                                                                                        queueTypeEnable && patientQueueData.queue_type_id == queue_type_id
                                                                                    )
                                        && patientQueueData.state_triage != 5
                                   orderby
                                         patientQueueData.time_interval,
                                         patientQueueData.is_priority descending,
                                         patientQueueData.sub_queue_order descending,
                                         patientQueueData.queue_num,
                                         patientQueueData.fre_date
                                   select patientQueueData;

            var listPriority = (from p in patientQueueList
                                where p.is_priority && !p.is_call
                                orderby p.opr_time
                                select p
                        ).Take(Count).ToList();
            if (listPriority.Count() == Count)
                return listPriority;

            var listFirst = (from p in patientQueueList
                             where !p.is_priority && p.state_call == 0 && p.state_triage == 0
                             select p).Take(Count).ToList();

            var listPass = (from p in patientQueueList
                            where !p.is_priority && p.state_call == 1 && !p.is_call
                            orderby p.opr_time
                            select p).Take(Count).ToList();

            var listReturn = (from p in patientQueueList
                              where !p.is_call && !p.is_priority && p.state_triage == 1 && p.state_call != 1
                              orderby p.opr_time
                              select p).Take(Count).ToList();

            var tmpFirst = from p in listFirst
                           select new
                           {
                               data = p,
                               index = listFirst.IndexOf(p)
                           };

            var tmpPass = from p in listPass
                          select new
                          {
                              data = p,
                              index = listPass.Take(listPass.IndexOf(p) + 1).Sum(t => t.return_flag).Value
                          };

            var listFirstAndPass = (from p in tmpFirst.Union(tmpPass)
                                    orderby p.index, p.data.state_call descending
                                    select p.data).ToList();

            var rtn = listPriority.Union(
                                        from patient in
                                            (
                                                (from p in listFirstAndPass
                                                 select new
                                                 {
                                                     data = p,
                                                     index = listFirstAndPass.IndexOf(p)
                                                 }).Union(
                                                from p in listReturn
                                                select new
                                                {
                                                    data = p,
                                                    index = listReturn.Take(listReturn.IndexOf(p) + 1).Sum(t => t.return_flag).Value
                                                }
                                                )
                                                )
                                        orderby patient.index, patient.data.state_triage descending
                                        select patient.data).Take(Count);

            return rtn;
        }
    }
}
