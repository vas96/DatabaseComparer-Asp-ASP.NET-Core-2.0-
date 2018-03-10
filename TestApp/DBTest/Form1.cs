using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Diagnostics;
using System.Threading;
using DBTest;
using MySql.Data.MySqlClient;

namespace DbComparer
{
    public partial class Form1 : Form
    {
        private bool sql = true;
        private static string sql_con = null;
        private string mysql_con = null;
        private static DataTable[] set = new DataTable[2];
        private static bool[] set_b = new bool[2];

        static Stopwatch sw = new Stopwatch();

        private DatabaseComparer dbComp;



        public Form1()
        {
            InitializeComponent();
            dbComp = new DatabaseComparer();
        }

        private void ComboBoxSelectedIndexChanged(object sender, EventArgs e)
        {
            Database db = null;
            TreeView tv;
            switch ((sender as ComboBox).SelectedIndex)
            {
                case 0:
                    {
                        db = new SqlDataBaseConnector();
                        break;
                    }
                case 1:
                    {
                        db = new MySqlDataBaseConnector();
                        break;
                    }
            }
            if ((sender as ComboBox) == comboBox1)
            {
                dbComp.FirstDatabase = db;
                tv = treeView1;
            }
            else
            {
                dbComp.SecondDatabase = db;
                tv = treeView2;
            }
            tv.Nodes.Clear();
            List<string> databases = db.GetDatabasesList();
            if (databases == null)
            {
                MessageBox.Show("Error");
                return;
            }
            foreach (var database in databases)
            {
                tv.Nodes.Add(new TreeNode(database));
            }
        }

        private void treeViewDoubleClick(object sender, EventArgs e)
        {
            TreeView tv = sender as TreeView;
            CheckedListBox clb;
            Database db;
            if (tv == treeView1)
            {
                clb = FirstSelectedCol;
                db = dbComp.FirstDatabase;
            }
            else
            {
                clb = SecondSelectedCol;
                db = dbComp.SecondDatabase;
            }
            if (tv.SelectedNode.Index >= 0 && tv.SelectedNode.Level == 0)
            {
                tv.SelectedNode.Nodes.Clear();
                clb.Items.Clear();
                db.SelectedTable = null;
                List<string> tables = db.GetTablesList(tv.SelectedNode.Text);
                db.SelectedDatabase = tv.SelectedNode.Text;
                TreeNode tn = tv.SelectedNode;
                foreach (var item in tables)
                {
                    tn.Nodes.Add(new TreeNode(item));
                }
            }
            if (tv.SelectedNode.Level > 0)
            {
                DataSet info = db.GetTableInfo(tv.SelectedNode.Text).DataSet;
                db.SelectedTable = tv.SelectedNode.Text;
                clb.Items.Clear();
                foreach (var row in info.Tables[0].Rows)
                {
                    clb.Items.Add((row as DataRow)[3]);
                }
            }
        }

        private void radioButtonCheckedChanged(object sender, EventArgs e)
        {
            RadioButton rb = sender as RadioButton;
            if (sender == radioButton1 || radioButton2 == sender)
            {
                if (radioButton1.Checked)
                    Thread.Sleep(100);
                //                    DatabaseComparer.GetComparer.FirstDatabase.ConnectToDatabase();
                else
                    dbComp.FirstDatabase.ConnectToServer();

            }
        }

        private void button1_Click(object sender, EventArgs e)
        {
            richTextBox1.Text = "";
            DatabaseComparer dc = dbComp;
//            dc.LoadFirstData("Select * from " + dc.FirstDatabase.SelectedTable, dc.FirstDatabase.FullSelector);
//            dc.LoadSecondData("Select * from " + dc.SecondDatabase.SelectedTable, dc.FirstDatabase.FullSelector);
            var list = dc.FindDifferences();
            foreach (var item in list)
            {
                richTextBox1.Text += item.ToString() + "\n";
            }

        }

        private void tabControl1_SelectedIndexChanged(object sender, EventArgs e)
        {
            DatabaseComparer dc = dbComp;
            if (dc.FirstDatabase != null && dc.SecondDatabase != null)
                panel5.Enabled = true;
        }

        private void button2_Click(object sender, EventArgs e)
        {
            richTextBox1.Text = "";
            DatabaseComparer dc = dbComp;
            foreach (var item in FirstSelectedCol.CheckedItems)
            {
                dc.FirstDatabase.SelectedColumns.Add(item.ToString());
            }
            foreach (var item in SecondSelectedCol.CheckedItems)
            {
                dc.SecondDatabase.SelectedColumns.Add(item.ToString());
            }
//            dc.LoadFirstData(dc.FirstDatabase.BuildSelectQuery(), dc.FirstDatabase.FullSelector);
//            dc.LoadSecondData(dc.SecondDatabase.BuildSelectQuery(), dc.SecondDatabase.FullSelector);
            var list = dc.FindDifferences();
            foreach (var item in list)
            {
                richTextBox1.Text += item + "\n";
            }
        }

        /*
private void button1_Click(object sender, EventArgs e)
{
try
{
listBox1.Items.Clear();
sw.Start();
using (var con = new SqlConnection("Data Source=.; Integrated Security=True;"))
{
con.Open();
DataTable databases = con.GetSchema("Databases");
foreach (DataRow database in databases.Rows)
{
String databaseName = database.Field<String>("database_name");
short dbID = database.Field<short>("dbid");
DateTime creationDate = database.Field<DateTime>("create_date");
listBox1.Items.Add(databaseName);
}
}
sql = true;
sw.Stop();
MessageBox.Show(sw.Elapsed.ToString());
}
catch (Exception)
{
MessageBox.Show("Помилка");
throw;
}
}

private void button2_Click(object sender, EventArgs e)
{
try
{
listBox1.Items.Clear();
sw.Start();
mysql_con = "SERVER=localhost;UID='root';" + "PASSWORD='';";
MySqlConnection connection = new MySqlConnection(mysql_con);
MySqlCommand command = connection.CreateCommand();
command.CommandText = "SHOW DATABASES;";
MySqlDataReader Reader;
connection.Open();
Reader = command.ExecuteReader();
while (Reader.Read())
{
string row = "";
for (int i = 0; i < Reader.FieldCount; i++)
listBox1.Items.Add(Reader.GetValue(i).ToString());
}
connection.Close();
sql = false;
sw.Stop();
MessageBox.Show(sw.Elapsed.ToString());
}
catch (Exception ee)
{
MessageBox.Show("Помилка:" + ee.Message);
throw;
}
}

private void button3_Click(object sender, EventArgs e)
{
if (sql == true)
{
try
{
sw.Start();
sql_con = "Data Source=.; Integrated Security=True; Initial Catalog =" +
listBox1.SelectedItem.ToString() + ";";
listBox1.Items.Clear();
using (var con = new SqlConnection(sql_con))
{
con.Open();
DataTable schema = con.GetSchema("Tables");
foreach (DataRow row in schema.Rows)
{
listBox1.Items.Add(row[2].ToString());
}
}
sw.Stop();
MessageBox.Show(sw.Elapsed.ToString());
}
catch (Exception ee)
{
MessageBox.Show(ee.Message);
throw;
}
}
else
{
try
{
sw.Start();
mysql_con = "SERVER=localhost;DATABASE=" + listBox1.SelectedItem.ToString() +
";UID=root; PASSWORD='';";
listBox1.Items.Clear();
using (MySqlConnection connection = new MySqlConnection(mysql_con))
{
MySqlCommand command = connection.CreateCommand();
command.CommandText = "SHOW TABLES;";
MySqlDataReader Reader;
connection.Open();
Reader = command.ExecuteReader();
List<string> rows = new List<string>();
while (Reader.Read())
{
for (int i = 0; i < Reader.FieldCount; i++)
listBox1.Items.Add(Reader.GetValue(i).ToString());
}
}
sw.Stop();
MessageBox.Show(sw.Elapsed.ToString());
}
catch (Exception ee)
{
MessageBox.Show(ee.Message);
throw;
}
}
}

private void button4_Click(object sender, EventArgs e)
{
if (sql == true)
{
set_b[0] = set_b[1] = false;
set[0] = set[1] = new DataTable();
sw = new Stopwatch();
sw.Start();
listView1.View = View.Details;
using (var con = new SqlConnection(sql_con))
{
con.Open();
SqlCommand com = new SqlCommand("select * from " + listBox1.SelectedItem.ToString(), con);
var colum_list = Functions.SQLgetColumnsName(con, (string) listBox1.SelectedItem);
listView1.Columns.Clear();
listView1.Items.Clear();
//                    foreach (var item in colum_list)
//                    {
//                        listView1.Columns.Add(item);
//                    }
Thread myThread = new Thread(new ThreadStart(Load));
myThread.Start(); // запускаем поток
//                    SqlDataAdapter da = new SqlDataAdapter();
//                    SqlCommand cmd = new SqlCommand("select * from NewEmployees", con);
//                    da.SelectCommand = cmd;
//                    DataSet ds = new DataSet();
//                    da.Fill(set[0]);
using (var da = new SqlDataAdapter("SELECT * FROM NewEmployees", con))
{
da.Fill(set[0]);
}
set_b[0] = true;
while (myThread.IsAlive)
{
}
//                    SqlCommand cmd2 = new SqlCommand("select * from NewEmployees2", con);
//                    da.SelectCommand = cmd;
//                    DataSet ds2 = new DataSet();
//                    da.Fill(ds2);
//                    System.Console.WriteLine(123);
//                    using (SqlDataReader reader = com.ExecuteReader())
//                    {
//                        while (reader.Read())
//                        {
//                            ListViewItem listitem = new ListViewItem(reader[colum_list[0]].ToString());
//                            for (int i=1; i<colum_list.Length;i++)
//                            {
//                                listitem.SubItems.Add(reader[colum_list[i]].ToString());
//                            }
//
//                            listView1.Items.Add(listitem);
//                        }
//                    }
}
sw.Stop();
MessageBox.Show(sw.Elapsed.ToString());
}
else
{
sw.Start();
listView1.View = View.Details;
using (var con = new MySqlConnection(mysql_con))
{
MySqlCommand com = new MySqlCommand("select * from " + listBox1.SelectedItem.ToString(), con);
con.Open();
var colum_list = Functions.MySQLgetColumnsName(mysql_con, listBox1.SelectedItem.ToString());
listView1.Items.Clear();
listView1.Columns.Clear();
foreach (var item in colum_list)
{
listView1.Columns.Add(item);
}
using (MySqlDataReader reader = com.ExecuteReader())
{
while (reader.Read())
{
ListViewItem listitem = new ListViewItem(reader[colum_list[0]].ToString());
for (int i = 1; i < colum_list.Length; i++)
{
listitem.SubItems.Add(reader[colum_list[i]].ToString());
}

listView1.Items.Add(listitem);
}
}
}
sw.Stop();
MessageBox.Show(sw.Elapsed.ToString());
}
}

public static void Load()
{
using (var con = new SqlConnection(sql_con))
{
con.Open();
DataTable dt = new DataTable();
using (var da = new SqlDataAdapter("SELECT * FROM NewEmployees2", con))
{
da.Fill(dt);
}
}
set_b[1] = true;
}

private void button5_Click(object sender, EventArgs e)
{
sw = new Stopwatch();
sw.Start();
int counter;
DbColumnInfo colum_list;
using (var con = new SqlConnection("Data Source=.; Integrated Security=True; Initial Catalog = Repair"))
{
con.Open();
SqlCommand com = new SqlCommand("Select * FROM NewEmployees", con);
colum_list = Functions.SQLgetColumnsName(con, "NewEmployees");
counter = colum_list.SqlDataType.Count;
}
DataTable dt=new DataTable();
for (int i = 0; i < counter; i++)
{
dt.Columns.Add(colum_list.Name[i], colum_list.SqlDataType[i]);
}
Func<IDataRecord, string> select = delegate(IDataRecord s)
{
//                var list = new dynamic[counter];
//                for (int i=0; i<counter;i++)
//                {
//                    list[i] = s[i];
//                }
//                return list;

//                return String.Format("{0} {1}", s[0], s[1]);

//                DataRow dr;
//                dr = dt.NewRow();
//                for (int i = 0; i < counter; i++)
//                {
//                    dr[colum_list.Name[i]] = s[i];
//                }
//                return dr;

//                string str="";
//                for (int i = 0; i < counter; i++)
//                {
//                    str += s[i] + " ";
//                }
//                return str.Remove(str.Length - 2, 2);

return String.Format("{0} {1}", s[0], s[1]);

//                return s;
};
//            var list15 = Read5("Select Id_Employee, Name FROM NewEmployees", select);
var list1 = Read4("Select Id_Employee, Name FROM NewEmployees", select);
//            var list13 = Read3("Select Id_Employee, Name FROM NewEmployees", select).ToArray();
//            var list1 = Read2<MySqlDataAdapter>("Select * FROM NewEmployees");
var list2 = Read4("Select Id_Employee, Name FROM NewEmployees2", select);
var list31 = list1.Except(list2);
var list32 = list2.Except(list1);
var lis3 = list31.Union(list32);
sw.Stop();
MessageBox.Show(sw.Elapsed.ToString());
}

public DataTable Read2<S>(string query) where S : IDbDataAdapter, IDisposable, new()
{
using (var conn = new SqlConnection("Data Source=.; Integrated Security=True; Initial Catalog = Repair"))
{
using (var da = new S())
{
using (da.SelectCommand = conn.CreateCommand())
{
da.SelectCommand.CommandText = query;
DataSet ds = new DataSet();
da.Fill(ds);
return ds.Tables[0];
}
}
}
}

public IEnumerable<S> Read3<S>(string query, Func<IDataRecord, S> selector)
{
using (var conn = new SqlConnection("Data Source=.; Integrated Security=True; Initial Catalog = Repair"))
{
using (var cmd = conn.CreateCommand())
{
cmd.CommandText = query;
cmd.Connection.Open();
using (var r = cmd.ExecuteReader())
while (r.Read())
yield return selector(r);
}
}
}


public S[] Read4<S>(string query, Func<IDataRecord, S> selector)
{
using (var conn = new SqlConnection("Data Source=.; Integrated Security=True; Initial Catalog = Repair"))
{
using (var cmd = conn.CreateCommand())
{
cmd.CommandText = query;
cmd.Connection.Open();
using (var r = cmd.ExecuteReader())
return ((DbDataReader)r).Cast<IDataRecord>().Select(selector).ToArray();
}
}
}

public List<S> Read5<S>(string query, Func<IDataRecord, S> selector)
{
using (var conn = new SqlConnection("Data Source=.; Integrated Security=True; Initial Catalog = Repair"))
{
using (var cmd = conn.CreateCommand())
{
cmd.CommandText = query;
cmd.Connection.Open();
using (var r = cmd.ExecuteReader())
{
var items = new List<S>();
while (r.Read())
items.Add(selector(r));
return items;
}
}
}
}

private void button6_Click(object sender, EventArgs e)
{
int aa=2;
var t = aa.GetType();
dynamic a=2;
MessageBox.Show(a);
//            if (a == b)
//                MessageBox.Show("yes");
//            else
//                MessageBox.Show("no");
}

*/
    }
}
