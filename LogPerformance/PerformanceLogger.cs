using Nest;
using System;
using System.Configuration;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
namespace LogPerformance
{
    public class PerformanceLogger
    {
        private static Boolean DisableLogging = true;

        //public static void LogEvent(string _eventName, string _regionName, EventAction ev, string _message, string _eventStartTimeStamp, string ShortcutKey, string _eventEndTimeStamp)

        public static void LogEvent(string _eventName, string _regionName, string _eventStartTimeStamp, string _eventEndTimeStamp, string sessionID, string documentid, string workqueueid, string userid, string msg, bool enableLogging = false)
        {


            //await Task.Run(() =>
            //{
                //if (DisableLogging)
                //    return;
                //string functionalityInvoked = System.Web.HttpContext.Current.Request.Headers["LoggingKey"];
                //string sessionDetails = System.Web.HttpContext.Current.Request.Headers["LoggingSessionDetails"];
                var server = new Uri(ConfigurationManager.AppSettings["LoggingStore"]);
                var settings = new ConnectionSettings(server);
                var settingAuthentication = settings.BasicAuthentication(ConfigurationManager.AppSettings["LoggingStoreId"], ConfigurationManager.AppSettings["LoggingStorePassword"]);
                var elastic = new ElasticClient(settings);
                //_regionName = "DataRoost_Performance";
                _eventName = _eventName + "_Timing";
                string dfsPath = String.Empty;
                string fileName = String.Empty;
                string WeekFolder = string.Join("_", DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day);
                byte[] byteArray = Encoding.ASCII.GetBytes(_eventStartTimeStamp);
                byte[] asciiArray = Encoding.Convert(Encoding.ASCII, Encoding.UTF8, byteArray);
                string finalString = Encoding.UTF8.GetString(asciiArray);
                _eventStartTimeStamp = finalString.Replace('?', ' ');
                byteArray = Encoding.ASCII.GetBytes(_eventEndTimeStamp);
                asciiArray = Encoding.Convert(Encoding.ASCII, Encoding.UTF8, byteArray);
                finalString = Encoding.UTF8.GetString(asciiArray);
                _eventEndTimeStamp = finalString.Replace('?', ' ');

                SessionUserEventMirror userEventDocument = new SessionUserEventMirror
                {
                    sessionID = sessionID,
                    documentID = documentid,
                    workQueueID = workqueueid,
                    userID = -1,
                    Iconum = -1,
                    UserName = null,
                    managerID = -1,
                    ManagerName = null,
                    Market = null,
                    jobType = null,
                    EnvironmentName = ConfigurationManager.AppSettings["FactSet.Fundamentals.InteractiveMultiAgent.Settings.EnvironmentName"],
                    Application = null,
                    WqType = null,
                    eventName = _eventName,
                    regionName = _regionName,
                    eventStartTimeStamp = _eventStartTimeStamp,
                    message = msg,
                    eventEndTimeStamp = _eventEndTimeStamp,
                    eventType = 2,
                    eventTypeName = null,
                    ShortcutKey = null
                };
                try
                {
                    for (int i = 1; i <= 10; i++)
                    {
                        var result = elastic.Index(userEventDocument, p => p
                                        .Index("scar")
                                        .Type("logging")
                                        );
                        if (result.IsValid)
                        {
                            Console.WriteLine("document inserted");
                            return;
                        }
                        else
                        {

                            Console.WriteLine("document Failed insertion");
                            return;
                        }
                        Thread.Sleep((Int32)Math.Pow(2, i) * 100);
                    }
                    #region WritetoFileSystemifWritetoDatabaseFails

                    #endregion
                }
                catch (Exception)
                {
                    Console.WriteLine("Unable to Write Log file to Log System.");

                }
            //});
        }
    }
    public class SessionUserEventMirror
    {
        public string sessionID { get; set; }
        public string documentID { get; set; }
        public string workQueueID { get; set; }
        public int userID { get; set; }
        public int Iconum { get; set; }
        public string UserName { get; set; }
        public int managerID { get; set; }
        public string ManagerName { get; set; }
        public string Market { get; set; }
        public int CompanyPriority { get; set; }
        public string IndustryDetail { get; set; }
        public string IndustryCode { get; set; }
        public string jobType { get; set; }
        public string EnvironmentName { get; set; }
        public string Application { get; set; }
        public string WqType { get; set; }
        public bool isPension { get; set; }

        public string UpdateType { get; set; }
        public string eventName { get; set; }
        public string regionName { get; set; }
        public string eventStartTimeStamp { get; set; }
        public string message { get; set; }
        public string eventEndTimeStamp { get; set; }
        public int eventType { get; set; }
        public string eventTypeName { get; set; }

        public string ShortcutKey { get; set; }
    }
}
