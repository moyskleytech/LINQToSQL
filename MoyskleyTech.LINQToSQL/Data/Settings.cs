using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MoyskleyTech.LINQToSQL.Data
{
    public class DatabaseSettings
    {
        public static DatabaseSettings Instance { get; set; }

        public string ConnectionString { get; set; }

    }
}
