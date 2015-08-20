using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.Odbc;


namespace Program
{
    /// <summary>
    //Database=dev; 
    //UID=masteruser; 
    //PWD=Passw0rd!; Port=5439
    /// </summary>
    /// <param name="_server">Server, e.g. "blackbox.cthwpi11m2oe.us-east-1.redshift.amazonaws.com"</param>
    /// <param name="_port">Port, e.g. "5439"</param>
    /// <param name="_masterUsername">MasterUserName, e.g. "masteruser"</param>
    /// <param name="_masterUserPassword">MasterUserPassword, e.g. "mypassword"</param>
    /// <param name="_DBName"> DBName, e.g. "devdatabase"</param>
    public class Redshift : IDisposable
    {

        OdbcConnection conn;

        // Server, e.g. "examplecluster.xyz.us-east-1.redshift.amazonaws.com"
        string server = "";
        // Port, e.g. "5439"
        string port = "";
        // MasterUserName, e.g. "masteruser".
        string masterUsername = "";
        // MasterUserPassword, e.g. "mypassword".
        string masterUserPassword = "";
        // DBName, e.g. "devdatabase"
        string DBName = "";
        string query = "";
        /// <summary>
        //Database=dev; 
        //UID=masteruser; 
        //PWD=Passw0rd!; Port=5439
        /// </summary>
        /// <param name="_server">Server, e.g. "blackbox.cthwpi11m2oe.us-east-1.redshift.amazonaws.com"</param>
        /// <param name="_port">Port, e.g. "5439"</param>
        /// <param name="_masterUsername">MasterUserName, e.g. "masteruser"</param>
        /// <param name="_masterUserPassword">MasterUserPassword, e.g. "mypassword"</param>
        /// <param name="_DBName"> DBName, e.g. "devdatabase"</param>
        public Redshift(string _server, string _port, string _masterUsername, string _masterUserPassword, string _DBName)
        {
            server = _server;
            port = _port;
            masterUsername = _masterUsername;
            masterUserPassword = _masterUserPassword;
            DBName = _DBName;
        }

        public void EnsureConnectionIsOpen()
        {

            if (conn == null)
            {

                // Create the ODBC connection string.
                string connString = "Driver={PostgreSQL Unicode};" +
            string.Format("Server={0};Database={1};" +
            "UID={2};PWD={3};Port={4};SSL=true;Sslmode=Require",
                    server, DBName, masterUsername,
                    masterUserPassword, port);

                // Make a connection using the psqlODBC provider.
                conn = new OdbcConnection(connString);
                conn.Open();

            }

        }

        public List<object[]> RunQuery(string _query, ref string Error)
        {

            DateTime init_time = DateTime.Now;
            // Create the ODBC connection string.  
            string connString = "Driver={PostgreSQL Unicode};" + string.Format("Server={0};Database={1};" + "UID={2};PWD={3};Port={4};SSL=true;Sslmode=Require", server, DBName, masterUsername, masterUserPassword, port);
            OdbcDataReader reader;
            List<object[]> Results = new List<object[]>();
            try
            {

                using (OdbcConnection connection = new OdbcConnection(connString))
                {
                    OdbcCommand myCommand = new OdbcCommand(_query, connection);

                    connection.Open();
                    reader = myCommand.ExecuteReader();
                    // Now we have an OdbcDataReader, we can find the number and names of the columns in the result set, and display this on the console.
                    Error = "Query executed succesfully" + Environment.NewLine;
                    int fCount = reader.FieldCount;
                    string[] resultquery = new string[fCount];
                    for (int i = 0; i < fCount; i++)
                    {
                        string fName = reader.GetName(i);
                        resultquery[i] = (fName);
                    }
                    string[] queryrow = new string[fCount];
                    resultquery.CopyTo(queryrow, 0);
                    Results.Add(queryrow);

                    // Now we can read each row from the result set, and read each column in that row and display its value.
                    while (reader.Read())
                    {
                        for (int i = 0; i < fCount; i++)
                        {
                            string col = (reader.IsDBNull(i)) ? "NULL" : (reader.GetValue(i).ToString());

                            resultquery[i] = (col);
                        }
                        string[] queryrow1 = new string[fCount];

                        resultquery.CopyTo(queryrow1, 0);
                        Results.Add(queryrow1);
                    }
                }
            }
            catch (Exception Query)
            {
                Exception FailedQuery = new Exception(string.Format("{0} failed query : {1}", Query.Message, _query));
                // Report.Error(710, FailedQuery);
                Error = "An error occurred when executing the SQL command: " + _query + Environment.NewLine + Query.Message + Environment.NewLine;
            }
            Error += string.Format("Execution time: {0}", (DateTime.Now - init_time).TotalSeconds.ToString("f2"));

            return Results;
        }
        public List<object> RunQuery(string _query)
        {

            // Create the ODBC connection string.  
            string connString = "Driver={PostgreSQL Unicode};" + string.Format("Server={0};Database={1};" + "UID={2};PWD={3};Port={4};SSL=true;Sslmode=Require", server, DBName, masterUsername, masterUserPassword, port);
            OdbcDataReader reader;

            List<object> Results = new List<object>();

            using (OdbcConnection connection = new OdbcConnection(connString))
            {
                OdbcCommand myCommand = new OdbcCommand(_query, connection);

                connection.Open();
                reader = myCommand.ExecuteReader();
                bool all_results = true;
                do
                {
                    object[] query_result = new object[25];
                    all_results = reader.Read();
                    if (all_results)
                    {
                        reader.GetValues(query_result);
                    }
                    // save the query_result to the list
                    Results.AddRange(query_result.Select(x => x).Where(q => q != null).ToList());


                    if (!all_results)
                    {
                        // get next result
                        all_results = reader.NextResult();
                    }
                } while (all_results); // loop throgh all responses 

            }



            return Results;
        }

        public List<string> RunCountQuery(string _query)
        {
            string connString = "Driver={PostgreSQL Unicode};" + string.Format("Server={0};Database={1};" + "UID={2};PWD={3};Port={4};SSL=true;Sslmode=Require", server, DBName, masterUsername, masterUserPassword, port);
            OdbcDataReader reader;

            List<string> Results = new List<string>();

            using (OdbcConnection connection = new OdbcConnection(connString))
            {
                OdbcCommand myCommand = new OdbcCommand(_query, connection);

                connection.Open();
                reader = myCommand.ExecuteReader();
                bool all_results = true;
                all_results = reader.Read();
                while (all_results)
                {            // loop throgh all responses              

                    // save the query_result to the list
                    Results.Add(reader[0].ToString());
                    all_results = reader.Read();
                    if (!all_results)
                    {
                        // get next result
                        all_results = reader.NextResult();
                        if (all_results) reader.Read();
                    }
                }
            }


            return Results;
        }


        public void Dispose()
        {
            if (conn != null)
                conn.Close();

        }
    }
}