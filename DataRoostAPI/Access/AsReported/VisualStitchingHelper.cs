using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using Npgsql;
using NpgsqlTypes;

namespace CCS.Fundamentals.DataRoostAPI.Access.AsReported
{
    public class VisualStitchingHelper
    {

        private readonly string _sfConnectionString;

        static VisualStitchingHelper()
        {

        }

        public VisualStitchingHelper(string sfConnectionString)
        {
            this._sfConnectionString = sfConnectionString;
        }

        private string factsetIOconnString = "Host=ip-172-31-81-210.manager.factset.io;Port=32791;Username=uyQKYrcTSrnnqB;Password=NoCLf_xBeXiB0UXZjhZUNg7Zx8;Database=di8UFb70sJdA5e;sslmode=Require;Trust Server Certificate=true;";
        private string connString = "Host=ffautomation-dev-postgres.c8vzac0v5wdo.us-east-1.rds.amazonaws.com;Port=5432;Username=ffautomation_writer_user;Password=qyp0nMeA;Database=postgres;"; // sslmode=Require;Trust Server Certificate=true;

        public string GetJson(int id)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            using (
                var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                // Retrieve all rows
                using (var cmd = new NpgsqlCommand("SELECT value FROM json where id = @id LIMIT 1", conn))
                {
                    cmd.Parameters.AddWithValue("id", id);
                    using (var reader = cmd.ExecuteReader())
                    { 
                        while (reader.Read())
                            sb.Append(reader.GetString(0));
                    }
                }
            }
            return sb.ToString();
        }
        public string GetJsonByHash(string hashkey)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            using (
                var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                // Retrieve all rows
                using (var cmd = new NpgsqlCommand("SELECT value FROM json where hashkey = @hashkey LIMIT 1", conn))
                {
                    cmd.Parameters.AddWithValue("@hashkey", hashkey);
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                            sb.Append(reader.GetString(0));
                    }
                }
            }
            return sb.ToString();
        }
        public int SetJsonByHash(string hashkey, string value)
        {
            string query = @"
UPDATE json SET value=@value WHERE hashkey=@hashkey;
INSERT INTO json (value, hashkey)
       SELECT @value, @hashkey
       WHERE NOT EXISTS (SELECT 1 FROM json WHERE  hashkey=@hashkey);

SELECT coalesce(id, -1) FROM json where hashkey = @hashkey LIMIT 1;

";
            int result = 0;
            try
            {
                using (
                    var conn = new NpgsqlConnection(connString))
                {
                    conn.Open();
                    // Retrieve all rows
                    using (var cmd = new NpgsqlCommand(query, conn))
                    {
                        cmd.Parameters.AddWithValue("@hashkey", hashkey);
                        cmd.Parameters.AddWithValue("@value", value);
                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                                result = reader.GetInt32(0);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                result = -1;
            }
            return result;
        }
    }
}
