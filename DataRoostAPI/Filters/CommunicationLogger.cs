using LogPerformance;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http.Controllers;
using System.Web.Http.Filters;

namespace CCS.Fundamentals.DataRoostAPI.CommLogger
{
    public class CommunicationLogger : ActionFilterAttribute
    {
        private static readonly ILogger Logger = LogManager.GetCurrentClassLogger();

        public static void LogEvent(string eventName, string regionName, /*EventAction ev, string _message,*/ string eventStartTimeStamp, string eventEndTimeStamp)
        {
            try
            {
                //await Task.Run(()=>
                //{
                string sessionID, documentid, workqueueid, userName, functionalityInvoked;


                if (HttpContext.Current != null && HttpContext.Current.User.Identity.IsAuthenticated)
                {
                    userName = HttpContext.Current.User.Identity.Name;
                }
                else
                {
                    userName = "DefaultUser";
                }
                if (HttpContext.Current != null && HttpContext.Current.Request.Headers["LoggingSessionDetails"] != null)
                {
                    string sessionDetails = HttpContext.Current.Request.Headers["LoggingSessionDetails"];
                    JObject jsonObject = JObject.Parse(sessionDetails);
                    sessionID = jsonObject.GetValue("sessionID").ToString();
                    documentid = jsonObject.GetValue("documentID").ToString();
                    workqueueid = jsonObject.GetValue("workqueueID").ToString();
                    functionalityInvoked = HttpContext.Current.Request.Headers["LoggingKey"];
                    DateTime StartDateStamp = Convert.ToDateTime(eventStartTimeStamp);
                    DateTime EndDateStamp = Convert.ToDateTime(eventEndTimeStamp);

                    JObject logMsg = new JObject();
                    jsonObject["ScarFunctionality"] = functionalityInvoked;
                    jsonObject["StartDateStamp"] = StartDateStamp;
                    jsonObject["EndDateStamp"] = EndDateStamp;

                    string loggedMsg = JsonConvert.SerializeObject(logMsg, Formatting.Indented);

                    if (regionName == "DataRoost")
                    {
                        regionName = "DataRoost_DB";
                    }

                    PerformanceLogger.LogEvent(eventName, regionName, StartDateStamp.ToString("yyyy-MM-dd HH:mm:ss"), EndDateStamp.ToString("yyyy-MM-dd HH:mm:ss"), sessionID, documentid, workqueueid, userName, loggedMsg);
                }
            }catch(Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
            //});
        }
        public override async Task OnActionExecutedAsync(HttpActionExecutedContext actionContext, CancellationToken cancellationToken)
        {
            await base.OnActionExecutedAsync(actionContext, cancellationToken);
            string userName = null;
            if (HttpContext.Current != null && HttpContext.Current.User.Identity.IsAuthenticated)
            {
                userName = HttpContext.Current.User.Identity.Name;
            }
            else
            {
                userName = "DefaultUser";
            }
            string StartTimeUtc = actionContext.Request.Properties[actionContext.ActionContext.ActionDescriptor.ActionName].ToString();
            string EndTimeUtc = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");
            string functionalityInvoked = System.Web.HttpContext.Current.Request.Headers["LoggingKey"];
            string sessionDetails = System.Web.HttpContext.Current.Request.Headers["LoggingSessionDetails"];

            if (functionalityInvoked != null && sessionDetails != null)
            {
                Console.WriteLine(sessionDetails);
                string filePath = System.Web.HttpContext.Current.Request.CurrentExecutionFilePath.ToString();
                JObject jsonObject = JObject.Parse(sessionDetails);
                string sessionID = jsonObject.GetValue("sessionID").ToString();
                string documentid = jsonObject.GetValue("documentID").ToString();
                string workqueueid = jsonObject.GetValue("workqueueID").ToString();
                JObject logMsg = new JObject();
                jsonObject["Route"] = filePath;
                jsonObject["StartDateStamp"] = StartTimeUtc;
                jsonObject["EndDateStamp"] = EndTimeUtc;
                string loggedMsg = JsonConvert.SerializeObject(logMsg, Formatting.Indented);
                addLoggingTiming(functionalityInvoked, "DataRoost_API", StartTimeUtc, EndTimeUtc, sessionID, documentid, workqueueid, loggedMsg, userName);
            }
        }

        public override async Task OnActionExecutingAsync(HttpActionContext actionContext, CancellationToken cancellationToken)
        {
            await base.OnActionExecutingAsync(actionContext, cancellationToken);
            actionContext.Request.Properties[actionContext.ActionDescriptor.ActionName] = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");
        }
        //public override bool AllowMultiple => true;
        private void addLoggingTiming(string eventName, string regionName, string eventStartTimeStamp, string eventEndTimeStamp, string sessionID, string documentid, string workqueueid, string msg, string userid)
        {
            PerformanceLogger.LogEvent(eventName, regionName, eventStartTimeStamp, eventEndTimeStamp, sessionID, documentid, workqueueid, userid, msg, true);
        }
    }



}