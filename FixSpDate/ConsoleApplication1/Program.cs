using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Program;

namespace Program
{
    class Program
    {
        public class StockSplitHistory
        {
            public int SecurityKey;
            public DateTime SplitDate;
            public double InverseSplitFactor;

            public StockSplitHistory(int sec_key, DateTime date, double factor)
            {
                this.SecurityKey = sec_key;
                this.SplitDate = date;
                this.InverseSplitFactor = factor;
            }
        }
        [STAThread]
        static void Main(string[] args)
        {
            // Read the Residual Data and load into memory filter only the espected profit greater than 0.005
            string path = "";
            var dialog = new OpenFileDialog();
            List<StockSplitHistory> Splits = new List<StockSplitHistory>();
            using (dialog)
            {
                if (dialog.ShowDialog() == DialogResult.OK)
                {
                    path = dialog.FileName;

                }
            }
            // code executed on opened excel file goes here.
            using (System.IO.StreamReader file = new System.IO.StreamReader(path))
            {
                string line = file.ReadLine(); // read header
                Redshift Redshift_Handler = new Redshift("blackbox.cy7bpvnuirbs.us-east-1.redshift.amazonaws.com", "5439", "masteruser", "Passw0rd!", "blackbox");
                while ((line = file.ReadLine()) != null)
                {
                    string[] values = line.Split(',');
                    int SecurityKey = int.Parse(values[0]);
                    DateTime SplitDate = DateTime.Parse(values[1]);
                    double oldshares = Convert.ToDouble(values[2]);
                    double newshares = Convert.ToDouble(values[3]);

                    double InverseSplitFactor = oldshares / newshares;
                    // new price = oldprice * InverseSplitFactor
                    StockSplitHistory Split = new StockSplitHistory(SecurityKey, SplitDate, InverseSplitFactor);
                    Splits.Add(Split);                                       

                }
                // Sort SlitDates
                Splits.OrderBy(x => x.SplitDate);

                foreach(StockSplitHistory Record in Splits)
                {
                    
                    // Query to fix Price
                    string query = "";
                    // Fundamentals Table
                    // Last_Split Date
                    query = string.Format("update fundamentals set last_split = market_date where market_date < '{0}' and sec_key = '{1}';", Record.SplitDate.ToString("yyyy/MM/dd"), Record.SecurityKey);
                    Redshift_Handler.RunQuery(query);
                    // Open Price
                    query = string.Format("update fundamentals set open_price = open_price * {1} where market_date < '{0}' and sec_key = '{2}';", Record.SplitDate.ToString("yyyy / MM / dd"), Record.InverseSplitFactor, Record.SecurityKey);
                    Redshift_Handler.RunQuery(query);
                    //previous_close
                    query = string.Format("update fundamentals set previous_close = previous_close * {1} where market_date < '{0}' and sec_key = '{2}';", Record.SplitDate.ToString("yyyy / MM / dd"), Record.InverseSplitFactor, Record.SecurityKey);
                    Redshift_Handler.RunQuery(query);
                    // high price
                    query = string.Format("update fundamentals set high_price = high_price * {1} where market_date < '{0}' and sec_key = '{2}';", Record.SplitDate.ToString("yyyy / MM / dd"), Record.InverseSplitFactor, Record.SecurityKey);
                    Redshift_Handler.RunQuery(query);
                    // low price
                    query = string.Format("update fundamentals set low_price = low_price * {1} where market_date < '{0}' and sec_key = '{2}';", Record.SplitDate.ToString("yyyy / MM / dd"), Record.InverseSplitFactor, Record.SecurityKey);
                    Redshift_Handler.RunQuery(query);
                    // previous_high
                    query = string.Format("update fundamentals set previous_high = previous_high * {1} where market_date < '{0}' and sec_key = '{2}';", Record.SplitDate.ToString("yyyy / MM / dd"), Record.InverseSplitFactor, Record.SecurityKey);
                    Redshift_Handler.RunQuery(query);
                    // previous_low
                    query = string.Format("update fundamentals set previous_low = previous_low * {1} where market_date < '{0}' and sec_key = '{2}';", Record.SplitDate.ToString("yyyy / MM / dd"), Record.InverseSplitFactor, Record.SecurityKey);
                    Redshift_Handler.RunQuery(query);

                    // Signals Table 
                    // last price
                    query = string.Format("update fundamentals set last_price = last_price * {1} where daytime < '{0}' and sec_key = '{2}';", Record.SplitDate.ToString("yyyy-MM-dd HH:mm:ss"), Record.InverseSplitFactor, Record.SecurityKey);
                    Redshift_Handler.RunQuery(query);
                    // bid
                    query = string.Format("update fundamentals set bid = bid * {1} where daytime < '{0}' and sec_key = '{2}';", Record.SplitDate.ToString("yyyy-MM-dd HH:mm:ss"), Record.InverseSplitFactor, Record.SecurityKey);
                    Redshift_Handler.RunQuery(query);
                    // ask
                    query = string.Format("update fundamentals set ask = ask * {1} where daytime < '{0}' and sec_key = '{2}';", Record.SplitDate.ToString("yyyy-MM-dd HH:mm:ss"), Record.InverseSplitFactor, Record.SecurityKey);
                    Redshift_Handler.RunQuery(query);

                }
                
            }
        }
    }
}
