using System;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Reflection;
using System.Linq;
using NLog;
using System.Web;
using System.Net.Http;
using System.Web.Http.Controllers;
using System.Web.Http.Filters;
using System.Threading;
using System.Threading.Tasks;
using System.Configuration;
using Nest;
//using Microsoft.IdentityModel.Clients.ActiveDirectory;

namespace CCS.Fundamentals.DataRoostAPI.CommLogger
{
    public class CommunicationLogger : ActionFilterAttribute
    {
        private static readonly ILogger Logger = LogManager.GetCurrentClassLogger();

        public override async Task OnActionExecutedAsync(HttpActionExecutedContext actionContext, CancellationToken cancellationToken)
        {
            await base.OnActionExecutedAsync(actionContext, cancellationToken);
            string userName = null;
            if (HttpContext.Current.User.Identity.IsAuthenticated)
            {
                userName = HttpContext.Current.User.Identity.Name;
            }
            else
            {
                userName = "DefaultUser";
            }
            Stopwatch stopwatch = (Stopwatch)actionContext.Request.Properties[actionContext.ActionContext.ActionDescriptor.ActionName];
            stopwatch.Stop();
            string timeElapsed = stopwatch.ElapsedMilliseconds.ToString();
            string functionalityInvoked = System.Web.HttpContext.Current.Request.Headers["LoggingKey"];
            string sessionId = System.Web.HttpContext.Current.Request.Headers["LoggingSessionId"];
            addLoggingTiming(timeElapsed, actionContext.ActionContext.Request.RequestUri.ToString(), userName, functionalityInvoked,sessionId);
        }

        public override async Task OnActionExecutingAsync(HttpActionContext actionContext, CancellationToken cancellationToken)
        {
            await base.OnActionExecutingAsync(actionContext, cancellationToken);
            actionContext.Request.Properties[actionContext.ActionDescriptor.ActionName] = Stopwatch.StartNew();
        }
        public override bool AllowMultiple
        {
            get { return true; }
        }
        private void addLoggingTiming(string timeElapsed, string requestUri, string user, string methodName, string sessionid){
            //Session session = new Session(requestUri, timeElapsed,user, methodName);
            

            Logger.Info(requestUri+ "-------------" + timeElapsed+ "-------------"+  user + "-------------"  + methodName + "--------" + sessionid);
        }
    }

    public class Session
    {
        public Guid sessionID { get; set; }
        public string requestUri { get; set; }
        public string timeElapsed { get; set; }
        public string methodName { get; set; }
        public string username { get; set; }
        
        public Session(string requestUri,string timeElapsed,string username, string methodName, bool requestInsert = false)
        {
            sessionID = new Guid();
            this.timeElapsed = timeElapsed;
            this.username = username;
            this.methodName = methodName;
            this.requestUri = requestUri;

            if (requestInsert)
            {
                postEventToElastic();
            }
           
        }

        public void postEventToElastic()
        {
            Session session = new Session(this.requestUri, this.timeElapsed, this.username, this.methodName);
            var server = new Uri(ConfigurationManager.AppSettings["LoggingStore"]);
            var settings = new ConnectionSettings(server);
            var settingAuthentication = settings.BasicAuthentication(ConfigurationManager.AppSettings["LoggingStoreId"], ConfigurationManager.AppSettings["LoggingStorePassword"]);
            var elastic = new ElasticClient(settings);
            var regionName = "DataRoost_SCAR_Performance";
            var result = elastic.Index(session, p => p
                                   .Index("scar")
                                   .Type("logging")
                                   );
            if (result.IsValid)
            {
                Console.WriteLine("document inserted");
                return;
            }
        }

        public void DestroySession()
        {
            this.sessionID = Guid.Empty;
        }


    }

}