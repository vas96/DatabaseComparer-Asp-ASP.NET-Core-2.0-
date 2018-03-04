using System;

using System.Data;



namespace DbComparer

{

    public class CompareDataTables

    {

        public static DataTable CompareTables(DataTable first, DataTable second)
        {
            first.TableName = "FirstTable";
            second.TableName = "SecondTable";

            //Create Empty Table

            DataTable table = new DataTable("Difference");
            try
            {
                //Must use a Dataset to make use of a DataRelation object
                using (DataSet ds = new DataSet())
                {
                    //Add tables
                    ds.Tables.AddRange(new DataTable[] { first.Copy(), second.Copy() });
                    //Get Columns for DataRelation
                    //DataColumn[] firstcolumns = new DataColumn[ds.Tables[0].Columns.Count];

                    DataColumn[] firstcolumns = new DataColumn[2];
                    firstcolumns[0] = ds.Tables[0].Columns[1];
                    firstcolumns[1] = ds.Tables[0].Columns[7];

                    //DataColumn[] secondcolumns = new DataColumn[ds.Tables[1].Columns.Count];
                    DataColumn[] secondcolumns = new DataColumn[2];
                    secondcolumns[0] = ds.Tables[1].Columns[1];
                    secondcolumns[1] = ds.Tables[1].Columns[7];

                    //Create DataRelation
                    DataRelation r = new DataRelation(string.Empty, firstcolumns, secondcolumns, false);
                    ds.Relations.Add(r);
                    //Create columns for return table
                    for (int i = 0; i < first.Columns.Count; i++)
                    {
                        table.Columns.Add(first.Columns[i].ColumnName, first.Columns[i].DataType);
                    }
                    //If First Row not in Second, Add to return table.
                    table.BeginLoadData();
                    foreach (DataRow parentrow in ds.Tables[0].Rows)
                    {
                        DataRow[] childrows = parentrow.GetChildRows(r);
                        if (childrows == null || childrows.Length == 0)
                            table.LoadDataRow(parentrow.ItemArray, true);
                    }
                    table.EndLoadData();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return table;
        }

    }

}