using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Data;
using System.Data.SqlClient;
using MySql.Data.MySqlClient;
using System.Configuration;

namespace MYSQLtoMONGODB
{
    class Program
    {
        static void Main(string[] args)
        {
            string connectionstring = "mongodb://localhost:27017";

            MongoClientSettings settings = MongoClientSettings.FromUrl(new MongoUrl(connectionstring));
            MongoClient mongoclient = new MongoClient(settings);
            var db = mongoclient.GetDatabase("ims");
            var bookcollection = db.GetCollection<BsonDocument>("ims");

            DataTable dt = new DataTable();

            string query1 = "select table_name from information_schema.tables where table_schema = 'ims'";
            Fill(dt, query1);

            MySqlConnectionStringBuilder conn_string = new MySqlConnectionStringBuilder();
            conn_string.Server = "localhost";
            conn_string.UserID = "root";
            conn_string.Password = "shivam123";
            conn_string.Database = "ims";
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                using (MySqlConnection conn = new MySqlConnection(conn_string.ToString()))
                {
                    string query = "select * from " + Convert.ToString(dt.Rows[i]["table_name"]);
                    using (MySqlCommand cmd = new MySqlCommand(query, conn))
                    {
                        conn.Open();
                        MySqlDataReader reader = cmd.ExecuteReader();
                        List<BsonDocument> bsonlist = new List<BsonDocument>(1000);

                        db.DropCollection(Convert.ToString(dt.Rows[i]["table_name"]), System.Threading.CancellationToken.None);
                        bookcollection = db.GetCollection<BsonDocument>(Convert.ToString(dt.Rows[i]["table_name"]));
                        while (reader.Read())
                        {
                            BsonDocument bson = new BsonDocument();
                            for (int j = 0; j < reader.FieldCount; j++)
                            {
                                if (reader[j].GetType() == typeof(string))
                                {
                                    bson.Add(new BsonElement(reader.GetName(j), reader[j].ToString()));
                                }
                                else if (reader[j].GetType() == typeof(Int32))
                                {
                                    bson.Add(new BsonElement(reader.GetName(j), BsonValue.Create(reader.GetInt32(j))));
                                }
                                else if (reader[j].GetType() == typeof(Int16))
                                {
                                    bson.Add(new BsonElement(reader.GetName(j), BsonValue.Create(reader.GetInt16(j))));
                                }
                                else if (reader[j].GetType() == typeof(Int64))
                                {
                                    bson.Add(new BsonElement(reader.GetName(j), BsonValue.Create(reader.GetInt64(j))));
                                }
                                else if (reader[j].GetType() == typeof(float))
                                {
                                    bson.Add(new BsonElement(reader.GetName(j), BsonValue.Create(reader.GetFloat(j))));
                                }
                                else if (reader[j].GetType() == typeof(Double))
                                {
                                    bson.Add(new BsonElement(reader.GetName(j), BsonValue.Create(reader.GetDouble(j))));
                                }
                                else if (reader[j].GetType() == typeof(DateTime))
                                {
                                    bson.Add(new BsonElement(reader.GetName(j), BsonValue.Create(reader.GetDateTime(j))));
                                }
                                else if (reader[j].GetType() == typeof(Guid))
                                {
                                    bson.Add(new BsonElement(reader.GetName(j), BsonValue.Create(reader.GetGuid(j))));
                                }
                                else if (reader[j].GetType() == typeof(Boolean))
                                {
                                    bson.Add(new BsonElement(reader.GetName(j), BsonValue.Create(reader.GetBoolean(j))));
                                }
                                else if (reader[j].GetType() == typeof(DBNull))
                                {
                                    bson.Add(new BsonElement(reader.GetName(j), BsonNull.Value));
                                }
                                else if (reader[j].GetType() == typeof(Byte))
                                {
                                    bson.Add(new BsonElement(reader.GetName(j), BsonValue.Create(reader.GetByte(j))));
                                }
                                else if (reader[j].GetType() == typeof(Byte[]))
                                {
                                    bson.Add(new BsonElement(reader.GetName(j), BsonValue.Create(reader[j] as Byte[])));
                                }
                                else if (reader[j].GetType() == typeof(Decimal))
                                {
                                    bson.Add(new BsonElement(reader.GetName(j), BsonValue.Create(reader.GetDecimal(j))));
                                }
                                else
                                    throw new Exception();
                            }
                            bsonlist.Add(bson);
                        }
                        bookcollection.InsertMany(bsonlist);
                    }
                }
            }
        }

        public static void Fill(DataTable dt, string query)
        {
            MySqlConnectionStringBuilder conn_string = new MySqlConnectionStringBuilder();
            conn_string.Server = "localhost";
            conn_string.UserID = "root";
            conn_string.Password = "shivam123";
            conn_string.Database = "ims";
            MySqlConnection oConnection = new MySqlConnection(conn_string.ToString());
            MySqlCommand oCommand = new MySqlCommand(query, oConnection);
            MySqlDataAdapter oAdapter = new MySqlDataAdapter();
            oAdapter.SelectCommand = oCommand;
            oConnection.Open();

            using (MySqlTransaction oTransaction = oConnection.BeginTransaction())
            {
                try
                {
                    oAdapter.SelectCommand.Transaction = oTransaction;
                    oAdapter.Fill(dt);
                    oTransaction.Commit();
                }
                catch
                {
                    oTransaction.Rollback();
                    throw;
                }
                finally
                {
                    if (oConnection.State == System.Data.ConnectionState.Open)
                        oConnection.Close();
                    oConnection.Dispose();
                    oAdapter.Dispose();
                }
            }
        }
    }
}
