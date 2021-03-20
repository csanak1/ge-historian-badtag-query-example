using System;
using System.Collections.Generic;
using System.Linq;

//using MESBadTagChecker.Code.Models;

using Historian = Proficy.Historian.ClientAccess.API;

namespace MESBadTagChecker.Code
{
    //from "Models"
    public class TagDataModel
    {
        public string TagName { get; set; }
        public string Quality { get; set; }
        public string TimeStamp { get; set; }
        public string Desc { get; set; }
        public string EGU { get; set; }
        public string LoEGU { get; set; }
        public string HiEGU { get; set; }
        public string Value { get; set; }
    }
    //

    public class HistorianDA
    {
        private const string IHistSrv = "yourhistorianservername";
    
        Historian.ServerConnection sc;

        private bool IsConnected
        {
            get { return sc.IsConnected(); }
        }

        public List<TagDataModel> GetBadTags(string[] plants)
        {
            var badTagDatas = new List<TagDataModel>();

            try
            {
                Connect();

                if (!IsConnected)
                    Connect();

                foreach (string plant in plants) //for the tagname mask
                {

                    Historian.TagQueryParams queryTags = new Historian.TagQueryParams { PageSize = 500 }; // PageSize is the batch size of the while loop below, not recommended to set higher than 500
                    Historian.ItemErrors itemErrors = new Historian.ItemErrors();
                    Historian.DataSet dataSet = new Historian.DataSet();

                    List<Historian.Tag> tagDatas = new List<Historian.Tag>();
                    List<Historian.Tag> tempTagDatas;

                    queryTags.Criteria.TagnameMask = $"BC.{plant}*"; //tagname mask
                    queryTags.Criteria.CollectionDisabled = false;
                    queryTags.Categories = Historian.Tag.Categories.Basic;

                    while (sc.ITags.Query(ref queryTags, out tempTagDatas))
                        tagDatas.AddRange(tempTagDatas);
                    tagDatas.AddRange(tempTagDatas);

                    for (int i = 0; i < tagDatas.Count; i++)
                    {
                        var badTagData = new TagDataModel
                        {
                            TagName = tagDatas[i].Name,
                            Desc = tagDatas[i].Description
                        };

                        badTagDatas.Add(badTagData);
                    }

                    queryTags = new Historian.TagQueryParams { PageSize = 500 };

                    queryTags.Criteria.TagnameMask = $"BC.{plant}*";
                    queryTags.Criteria.CollectionDisabled = false;
                    queryTags.Categories = Historian.Tag.Categories.Engineering; //for engineering units and limits

                    tagDatas.Clear();
                    tempTagDatas.Clear();

                    string[] tagNames = badTagDatas.AsEnumerable().Select(r => r.TagName).ToArray(); //get only tagnames

                    while (sc.ITags.Query(ref queryTags, out tempTagDatas))
                        tagDatas.AddRange(tempTagDatas);
                    tagDatas.AddRange(tempTagDatas);

                    for (int i = 0; i < tagDatas.Count; i++)
                    {
                        var obj = badTagDatas.FirstOrDefault(x => x.TagName == tagDatas[i].Name); //get object by tagname
                        if (obj != null)
                        {
                            obj.EGU = tagDatas[i].EngineeringUnits;
                            obj.LoEGU = tagDatas[i].LoEngineeringUnits.ToString();
                            obj.HiEGU = tagDatas[i].HiEngineeringUnits.ToString();
                        }
                    }

                    // query the latest values
                    Historian.DataQueryParams queryValueQuality = new Historian.CurrentValueQuery(tagNames) { Fields = Historian.DataFields.Value | Historian.DataFields.Quality | Historian.DataFields.Time };

                    sc.IData.Query(ref queryValueQuality, out dataSet, out itemErrors);

                    for (int i = 0; i < tagNames.Length; i++)
                    {
                        var obj = badTagDatas.FirstOrDefault(x => x.TagName == tagNames[i]);

                        if (obj != null)
                        {
                            obj.Value = dataSet[tagNames[i]].GetValue(0) != null ? dataSet[tagNames[i]].GetValue(0).ToString() : "-N/A-";
                            obj.Quality = dataSet[tagNames[i]].GetQuality(0).ToString();
                            obj.TimeStamp = dataSet[tagNames[i]].GetTime(0).ToString("yyyy.MM.dd. HH:mm:ss");
                        }
                    }

                    badTagDatas = badTagDatas.Where(x => x.Quality.Contains("Bad")).ToList(); // filter to get only BAD qulity tags
                }

                return badTagDatas;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Tag query error: " + ex.Message);
                throw;
            }
            finally
            {
                Disconnect();
            }
        }

        // method to get the calculation for a tag (if exists)
        public string GetCalculationScript(string tagName)
        {
            string calc = string.Empty;

            try
            {
                Connect();

                if (!IsConnected)
                    Connect();

                Historian.TagQueryParams queryTags = new Historian.TagQueryParams { PageSize = 500 };

                List<Historian.Tag> tagDatas = new List<Historian.Tag>();
                List<Historian.Tag> tempTagDatas;

                queryTags.Criteria.TagnameMask = tagName;
                queryTags.Categories = Historian.Tag.Categories.All; // "All" to get the calculation field text

                while (sc.ITags.Query(ref queryTags, out tempTagDatas))
                    tagDatas.AddRange(tempTagDatas);
                tagDatas.AddRange(tempTagDatas);

                for (int i = 0; i < tagDatas.Count; i++)
                {
                    calc = tagDatas[i].Calculation.ToString();
                }

                return calc;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Calculation query error: " + ex.Message);
                throw;
            }
            finally
            {
                Disconnect();
            }
        }

        private void Connect()
        {
            if (sc == null)
            {
                sc = new Historian.ServerConnection(new Historian.ConnectionProperties
                {
                    ServerHostName = IHistSrv,
                    OpenTimeout = new TimeSpan(0, 0, 10),
                    ServerCertificateValidationMode = Historian.CertificateValidationMode.None
                });
            }

            if (!sc.IsConnected())
                try
                {
                    sc.Connect();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error at connecting: " + ex.Message);
                    throw;
                }
        }

        private void Disconnect()
        {
            Dispose();

            if (sc.IsConnected())
                try
                {
                    sc.Disconnect();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error at disconnecting: " + ex.Message);
                    throw;
                }
        }

        private void Dispose()
        {
            ((IDisposable)sc).Dispose();
        }
    }
}
