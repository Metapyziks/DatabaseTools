using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;
using System.Reflection;
using System.IO;
using System.Linq.Expressions;
using System.Diagnostics;

#if LINUX
    using DBConnection = Mono.Data.Sqlite.SqliteConnection;
    using DBCommand = Mono.Data.Sqlite.SqliteCommand;
    using DBDataReader = Mono.Data.Sqlite.SqliteDataReader;
#else
    using DBConnection = System.Data.SqlServerCe.SqlCeConnection;
    using DBCommand = System.Data.SqlServerCe.SqlCeCommand;
    using DBDataReader = System.Data.SqlServerCe.SqlCeDataReader;
    using DBEngine = System.Data.SqlServerCe.SqlCeEngine;
#endif

namespace DatabaseTools
{
    public class LoggedMessageEventArgs : EventArgs
    {
        public EventLogEntryType Type { get; private set; }
        public String Message { get; private set; }

        public LoggedMessageEventArgs(EventLogEntryType type, String message)
        {
            Message = message;
            Type = type;
        }
    }

    public delegate void LoggedMessageHandler(LoggedMessageEventArgs e);

    public static class Database
    {
        public static event LoggedMessageHandler LoggedMessage;

        public static bool VerboseLogging { get; set; }

        internal static void Log(EventLogEntryType type, String format, params object[] args)
        {
            var message = args.Length == 0 ? format : String.Format(format, args);

            if (LoggedMessage != null) {
                LoggedMessage(new LoggedMessageEventArgs(type, message));
            }
        }

        internal static void Log(Exception e)
        {
            Log(EventLogEntryType.Error, "{0}: {1}", e.Message, e.StackTrace);
        }

        internal static void Log(String format, params Object[] args)
        {
            Log(EventLogEntryType.Information, format, args);
        }

        internal static void LogVerbose(String format, params Object[] args)
        {
            if (!VerboseLogging) return;

            Log(EventLogEntryType.Information, format, args);
        }

        public static bool IsDefined<T>(this MemberInfo minfo, bool inherit = false)
            where T : Attribute
        {
            return minfo.IsDefined(typeof(T), inherit);
        }

        public static T GetCustomAttribute<T>(this MemberInfo minfo, bool inherit = false)
            where T : Attribute
        {
            T[] attribs = (T[]) minfo.GetCustomAttributes(typeof(T), inherit);

            if (attribs.Length == 0)
                return null;

            return attribs[0];
        }

        public static CultureInfo CultureInfo = new CultureInfo("en-US");

        public static String FileName
        {
            get { return Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "Database.sdf"); }
        }

        private static DBConnection _sConnection;

        private static readonly List<DatabaseTable> _sTables = new List<DatabaseTable>();

        public static void Connect(String connStrFormat, params String[] args)
        {
            if (_sConnection != null)
                Disconnect();

            Log("Establishing database connection...");
            String connectionString = String.Format(connStrFormat, args);
            _sConnection = new DBConnection(connectionString);
            _sConnection.Open();

            Type[] types;

            try {
                types = Assembly.GetEntryAssembly().GetTypes();
            } catch (ReflectionTypeLoadException e) {
                Log(e);
                throw;
            }

            foreach (Type type in types) {
                if (type.IsDefined<DatabaseEntityAttribute>()) {
                    DatabaseTable table = CreateTable(type);
                    table.BuildColumns();
                    LogVerbose("- Initialized table {0}", table.Name);
                }
            }

            foreach (var table in _sTables) {
                table.ResolveForeignKeys();
                if (!TableExists(table)) {
                    table.Create();
                }
            }
        }

        public static void ConnectLocal()
        {
            //#if DEBUG
            //            DropDatabase();
            //#endif

            if (!File.Exists(FileName))
                CreateDatabase("Data Source={0};", FileName);
            else
                Connect("Data Source={0};", FileName);
        }

        public static void DropDatabase()
        {
            if (_sConnection != null)
                Disconnect();

            if (File.Exists(FileName))
                File.Delete(FileName);
        }

        private static void CreateDatabase(String connStrFormat, params String[] args)
        {
#if !LINUX
            DBEngine engine = new DBEngine(String.Format(connStrFormat, args));
            engine.CreateDatabase();
            engine.Dispose();
#endif
            Connect(connStrFormat, args);
        }

        private static DatabaseTable CreateTable<T>()
            where T : new()
        {
            return CreateTable(typeof(T));
        }

        private static DatabaseTable CreateTable(Type type)
        {
            DatabaseTable newTable = new DatabaseTable(type);
            _sTables.Add(newTable);
            return newTable;
        }

        private static bool RequiresParam(Expression exp)
        {
            if (exp is UnaryExpression)
                return RequiresParam(((UnaryExpression) exp).Operand);

            if (exp is BinaryExpression) {
                BinaryExpression bExp = (BinaryExpression) exp;
                return RequiresParam(bExp.Left) || RequiresParam(bExp.Right);
            }

            if (exp is MemberExpression) {
                MemberExpression mExp = (MemberExpression) exp;
                return mExp.Expression != null && RequiresParam(mExp.Expression);
            }

            if (exp is MethodCallExpression) {
                MethodCallExpression mcExp = (MethodCallExpression) exp;
                return mcExp.Arguments.Any(x => RequiresParam(x));
            }

            if (exp is ParameterExpression)
                return true;

            if (exp is ConstantExpression)
                return false;

            throw new Exception("Cannot reduce an expression of type " + exp.GetType());
        }

        public static String Escape(this String str)
        {
            StringBuilder escaped = new StringBuilder();
            foreach (char c in str) {
                if (c == '\'') escaped.Append("''");
                else escaped.Append(c);
            }
            return escaped.ToString();
        }

        private static String SerializeExpression(Expression exp, bool removeParam = false)
        {
            if (!RequiresParam(exp)) {
                if (exp.Type == typeof(bool)) {
                    Expression<Func<bool,String>> toString = x => x ? "'1'='1'" : "'1'='0'";
                    return Expression.Lambda<Func<String>>(
                        Expression.Invoke(toString, exp)).Compile()();
                } else if (exp.Type == typeof(int)) {
                    Expression<Func<int,String>> toString = x => x.ToString();
                    return FormatValue(Expression.Lambda<Func<String>>(
                        Expression.Invoke(toString, exp)).Compile()());
                } else if (exp.Type == typeof(ulong)) {
                    Expression<Func<ulong,String>> toString = x => x.ToString();
                    return FormatValue(Expression.Lambda<Func<String>>(
                        Expression.Invoke(toString, exp)).Compile()());
                } else if (exp.Type == typeof(double)) {
                    Expression<Func<double,String>> toString = x => x.ToString();
                    return FormatValue(Expression.Lambda<Func<String>>(
                        Expression.Invoke(toString, exp)).Compile()());
                } else if (exp.Type == typeof(DateTime)) {
                    Expression<Func<DateTime,String>> toString = x => x.Ticks.ToString();
                    return FormatValue(Expression.Lambda<Func<String>>(
                        Expression.Invoke(toString, exp)).Compile()());
                } else {
                    return FormatValue(Expression.Lambda<Func<Object>>(exp).Compile()());
                }
            }

            if (exp is UnaryExpression) {
                UnaryExpression uExp = (UnaryExpression) exp;
                String oper = SerializeExpression(uExp.Operand, removeParam);

                switch (exp.NodeType) {
                    case ExpressionType.Not:
                        return String.Format("(NOT {0})", oper);
                    case ExpressionType.Convert:
                        return SerializeExpression(uExp.Operand, removeParam);
                    default:
                        throw new Exception("Cannot convert an expression of type "
                            + exp.NodeType + " to SQL");
                }
            }

            if (exp is BinaryExpression) {
                BinaryExpression bExp = (BinaryExpression) exp;
                String left = SerializeExpression(bExp.Left, removeParam);
                String right = SerializeExpression(bExp.Right, removeParam);
                switch (exp.NodeType) {
                    case ExpressionType.Equal:
                        return String.Format("({0} = {1})", left, right);
                    case ExpressionType.NotEqual:
                        return String.Format("({0} != {1})", left, right);
                    case ExpressionType.LessThan:
                        return String.Format("({0} < {1})", left, right);
                    case ExpressionType.LessThanOrEqual:
                        return String.Format("({0} <= {1})", left, right);
                    case ExpressionType.GreaterThan:
                        return String.Format("({0} > {1})", left, right);
                    case ExpressionType.GreaterThanOrEqual:
                        return String.Format("({0} >= {1})", left, right);
                    case ExpressionType.AndAlso:
                        return String.Format("({0} AND {1})", left, right);
                    case ExpressionType.OrElse:
                        return String.Format("({0} OR {1})", left, right);
                    default:
                        throw new Exception("Cannot convert an expression of type "
                            + exp.NodeType + " to SQL");
                }
            }

            switch (exp.NodeType) {
                case ExpressionType.Parameter:
                    var pExp = (ParameterExpression) exp;
                    return pExp.Name;
                case ExpressionType.Constant:
                    var cExp = (ConstantExpression) exp;
                    return FormatValue(cExp.Value);
                case ExpressionType.MemberAccess:
                    var mExp = (MemberExpression) exp;
                    if (removeParam && mExp.Expression is ParameterExpression)
                        return mExp.Member.Name;
                    if (mExp.Expression is ParameterExpression) {
                        var param = (ParameterExpression) mExp.Expression;
                        var paramName = param.Name;
                        DatabaseTable table = GetTable(param.Type);
                        if (!table.Columns.Any(x => x.Name == mExp.Member.Name)) {
                            while ((table = table.SuperTable) != null) {
                                if (table.Columns.Any(x => x.Name == mExp.Member.Name)) {
                                    paramName = table.Name;
                                    break;
                                }
                            }
                        }
                        return String.Format("{0}.{1}", paramName, mExp.Member.Name);
                    }
                    return String.Format("{0}.{1}", SerializeExpression(
                            mExp.Expression, removeParam),
                        mExp.Member.Name);
                default:
                    throw new Exception("Cannot convert an expression of type "
                        + exp.NodeType + " to SQL");
            }
        }

        public static int ExecuteNonQuery(String format, params Object[] args)
        {
            var cmd = new DBCommand(String.Format(format, args), _sConnection);
            LogVerbose(cmd.CommandText);
            return cmd.ExecuteNonQuery();
        }

        public static DatabaseTable GetTable<T>()
        {
            return GetTable(typeof(T));
        }

        public static DatabaseTable GetTable(Type t)
        {
            return _sTables.FirstOrDefault(x => x.Type == t);
        }

        public static DatabaseTable[] GetTables()
        {
            return _sTables.ToArray();
        }

        public static bool TableExists<T>()
        {
            return TableExists(GetTable(typeof(T)));
        }

        public static bool TableExists(Type t)
        {
            return TableExists(GetTable(t));
        }

        public static bool TableExists(DatabaseTable table)
        {
#if LINUX
            String statement = String.Format("SELECT * FROM sqlite_master " +
                "WHERE type = 'table' AND name = '{0}'", table.Name);
#else
            String statement = String.Format("SELECT * FROM INFORMATION_SCHEMA.TABLES " +
                "WHERE TABLE_NAME = '{0}'", table.Name);
#endif
            DBCommand cmd = new DBCommand(statement, _sConnection);
            using (var reader = cmd.ExecuteReader()) {
                return reader.Read();
            }
        }

        private static String FormatValue(Object value)
        {
            if (value == null) return "NULL";
            return String.Format("'{0}'", value.ToString().Escape());
        }

        private static void GenerateTableDependencies(DatabaseTable table, String alias, List<String> from, List<String> columns)
        {
            columns.AddRange(table.Columns.Select(x => alias + "." + x.Name));
            from.Add(table.Name + " AS " + alias);
            var oldPrimary = table.Columns.First(x => x.PrimaryKey);
            DatabaseTable super = table;
            while ((super = super.SuperTable) != null) {
                columns.AddRange(super.Columns.Select(x => super.Name + "." + x.Name));
                var primary = super.Columns.First(x => x.PrimaryKey);
                from.Add(String.Format("INNER JOIN {0} ON {0}.{2} = {1}.{3}",
                    super.Name, alias, primary.Name, oldPrimary.Name));
                oldPrimary = primary;
            }
        }

        private static DBCommand GenerateSelectCommand<T>(DatabaseTable table,
            bool selectLast, params Expression<Func<T, bool>>[] predicates)
            where T : new()
        {
            for (int i = 1; i < predicates.Length; ++i)
                if (predicates[i].Parameters[0].Name != predicates[0].Parameters[0].Name)
                    throw new Exception("All predicates must use the same parameter name");

            String alias = predicates[0].Parameters[0].Name;

            var from = new List<String>();
            var columns = new List<String>();

            GenerateTableDependencies(table, alias, from, columns);

            StringBuilder builder = new StringBuilder();
            builder.AppendFormat("SELECT\n  {0}\nFROM {1}\n", String.Join(", ", columns),
                String.Join("\n  ", from));

            builder.AppendFormat("WHERE {0}\n", String.Join("\n  OR ",
                predicates.Select(x => SerializeExpression(x.Body))));

            if (selectLast) {
                builder.AppendFormat("ORDER BY {0}.{1} DESC\n", alias, table.Columns.First(x => x.PrimaryKey).Name);
            }

            LogVerbose(builder.ToString());

            return new DBCommand(builder.ToString(), _sConnection);
        }

        private static DBCommand GenerateSelectCommand<T0, T1>(DatabaseTable table0,
            DatabaseTable table1, Expression<Func<T0, T1, bool>> joinOn, params Expression<Func<T0, T1, bool>>[] predicates)
            where T0 : new()
            where T1 : new()
        {
            for (int i = 1; i < predicates.Length; ++i) {
                if (predicates[i].Parameters[0].Name != predicates[0].Parameters[0].Name
                    || predicates[i].Parameters[1].Name != predicates[0].Parameters[1].Name) {
                    throw new Exception("All predicates must use the same parameter name");
                }
            }

            var alias0 = predicates[0].Parameters[0].Name;
            var alias1 = predicates[0].Parameters[1].Name;

            var columns = String.Join(",\n  ", table0.Columns.Select(x => alias0 + "." + x.Name))
                + ",\n  " + String.Join(",\n  ", table1.Columns.Select(x => alias1 + "." + x.Name));

            var builder = new StringBuilder();
            builder.AppendFormat("SELECT\n  {0}\nFROM {1} AS {2}\nINNER JOIN {3} AS {4}\nON {5}\n", columns,
                table0.Name, alias0, table1.Name, alias1, SerializeExpression(joinOn.Body));

            builder.AppendFormat("WHERE {0}", String.Join("\n  OR ",
                predicates.Select(x => SerializeExpression(x.Body))));

            LogVerbose(builder.ToString());

            return new DBCommand(builder.ToString(), _sConnection);
        }

        private static T ReadEntity<T>(this DBDataReader reader, DatabaseTable table)
            where T : new()
        {
            return (T) ReadEntity(reader, table);
        }

        private static Object ReadEntity(this DBDataReader reader, DatabaseTable table)
        {
            Object entity = null;

            if (reader.Read()) {
                entity = table.Type.GetConstructor(new Type[] { }).Invoke(new Object[] { });

                do {
                    foreach (DatabaseColumn col in table.Columns)
                        col.SetValue(entity, reader[col.Name]);
                } while ((table = table.SuperTable) != null);
            }

            return entity;
        }

        private static Tuple<T0, T1> ReadEntity<T0, T1>(this DBDataReader reader, DatabaseTable table0, DatabaseTable table1)
            where T0 : new()
            where T1 : new()
        {
            Tuple<T0, T1> entity = null;

            if (reader.Read()) {
                entity = new Tuple<T0, T1>(new T0(), new T1());

                foreach (DatabaseColumn col in table0.Columns)
                    col.SetValue(entity.Item1, reader[col.Name]);

                foreach (DatabaseColumn col in table1.Columns)
                    col.SetValue(entity.Item2, reader[col.Name]);
            }

            return entity;
        }

        public static int Begin()
        {
#if LINUX
            return ExecuteNonQuery("BEGIN");
#else
            // return ExecuteNonQuery("BEGIN TRANSACTION");
            return 1;
#endif
        }

        public static int End()
        {
#if LINUX
            return ExecuteNonQuery("END");
#else
            // return ExecuteNonQuery("COMMIT TRANSACTION");
            return 1;
#endif
        }

        /// <summary>
        /// Returns the first item from the specified table that matches
        /// all given predicates, or null if no items do.
        /// </summary>
        /// <typeparam name="T">Entity type of the table to select from</typeparam>
        /// <param name="predicates">Predicates for the selected item to match</param>
        /// <returns>The first item that matches all given predicates, or null</returns>
        public static T SelectFirst<T>(params Expression<Func<T, bool>>[] predicates)
            where T : class, new()
        {
            DatabaseTable table = GetTable<T>();
            DBCommand cmd = GenerateSelectCommand(table, false, predicates);

            T entity;
            using (DBDataReader reader = cmd.ExecuteReader()) {
                if (!reader.HasRows) return null;
                entity = reader.ReadEntity<T>(table);
            }

            return entity;
        }

        public static T SelectLast<T>(params Expression<Func<T, bool>>[] predicates)
            where T : class, new()
        {
            DatabaseTable table = GetTable<T>();
            DBCommand cmd = GenerateSelectCommand(table, true, predicates);

            T entity;
            using (DBDataReader reader = cmd.ExecuteReader()) {
                if (!reader.HasRows) return null;
                entity = reader.ReadEntity<T>(table);
            }

            return entity;
        }

        private static Object SelectLast(DatabaseTable table)
        {
            DBCommand cmd = GenerateSelectCommand<Object>(table, true, x => true);

            Object entity;
            using (DBDataReader reader = cmd.ExecuteReader()) {
                if (!reader.HasRows) return null;
                entity = reader.ReadEntity(table);
            }

            return entity;
        }

        /// <summary>
        /// Returns the first pair of items from the cartesian product of two
        /// tables that matches all given predicates, or null if no pairs do.
        /// </summary>
        /// <typeparam name="T0">Entity type of the first table to select from</typeparam>
        /// <typeparam name="T1">Entity type of the second table to select from</typeparam>
        /// <param name="predicates">Predicates for the </param>
        /// <returns></returns>
        public static Tuple<T0, T1> SelectFirst<T0, T1>(Expression<Func<T0, T1, bool>> joinOn, params Expression<Func<T0, T1, bool>>[] predicates)
            where T0 : new()
            where T1 : new()
        {
            var table0 = GetTable<T0>();
            var table1 = GetTable<T1>();
            var cmd = GenerateSelectCommand(table0, table1, joinOn, predicates);

            Tuple<T0, T1> entity;
            using (DBDataReader reader = cmd.ExecuteReader())
                entity = reader.ReadEntity<T0, T1>(table0, table1);

            return entity;
        }

        public static List<T> Select<T>(params Expression<Func<T, bool>>[] predicates)
            where T : new()
        {
            if (predicates.Length == 0) return new List<T>();

            DatabaseTable table = GetTable<T>();
            DBCommand cmd = GenerateSelectCommand(table, false, predicates);

            var entities = new List<T>();
            using (DBDataReader reader = cmd.ExecuteReader()) {
                T entity;
                while ((entity = reader.ReadEntity<T>(table)) != null)
                    entities.Add(entity);
            }

            return entities;
        }

        public static List<Tuple<T0, T1>> Select<T0, T1>(Expression<Func<T0, T1, bool>> joinOn, params Expression<Func<T0, T1, bool>>[] predicates)
            where T0 : new()
            where T1 : new()
        {
            if (predicates.Length == 0) return new List<Tuple<T0, T1>>();

            var table0 = GetTable<T0>();
            var table1 = GetTable<T1>();
            var cmd = GenerateSelectCommand(table0, table1, joinOn, predicates);

            var entities = new List<Tuple<T0, T1>>();
            using (DBDataReader reader = cmd.ExecuteReader()) {
                Tuple<T0, T1> entity;
                while ((entity = reader.ReadEntity<T0, T1>(table0, table1)) != null)
                    entities.Add(entity);
            }

            return entities;
        }

        public static List<T> SelectAll<T>()
            where T : new()
        {
            return Select<T>(x => true);
        }

        private static IEnumerable<object> SelectAll(DatabaseTable table)
        {
            var cmd = GenerateSelectCommand<Object>(table, false, x => true);

            var entities = new List<Object>();
            using (var reader = cmd.ExecuteReader()) {
                Object entity;
                while ((entity = reader.ReadEntity(table)) != null)
                    entities.Add(entity);
            }

            return entities;
        }

        public static List<Tuple<T0, T1>> SelectAll<T0, T1>(Expression<Func<T0, T1, bool>> joinOn)
            where T0 : new()
            where T1 : new()
        {
            return Select(joinOn, (x, y) => true);
        }

        public static int Insert<T>(T entity)
            where T : new()
        {
            return Insert(GetTable<T>(), entity);
        }

        private static int Insert(DatabaseTable table, Object entity)
        {
            if (table.SuperTable != null) {
                Insert(table.SuperTable, entity);
                var inherited = table.Columns.Where(x => !x.AutoIncrement
                    && table.SuperTable.Columns.Any(y => y.Name == x.Name))
                    .Select(x => Tuple.Create(
                        x, table.SuperTable.Columns.First(y => y.Name == x.Name)));

                if (inherited.Any()) {
                    var super = SelectLast(table.SuperTable);
                    foreach (var pair in inherited) {
                        pair.Item1.SetValue(entity, pair.Item2.GetValue(super));
                    }
                }
            }

            var nonAuto = table.Columns.Where(x => !x.AutoIncrement).ToArray();

            var columns = String.Join(",\n  ", nonAuto.Select(x => x.Name));
            var values = String.Join(",\n  ", nonAuto.Select(x => FormatValue(x.GetValue(entity))));

            var result = ExecuteNonQuery("INSERT INTO {0}\n(\n  {1}\n) VALUES (\n  {2}\n)", table.Name, columns, values);

            var auto = table.Columns.FirstOrDefault(x => x.AutoIncrement && x.PrimaryKey);
            if (auto == null) return result;

            var cmd = new DBCommand("SELECT @@IDENTITY AS ID", _sConnection);
            var res = cmd.ExecuteScalar();

            auto.SetValue(entity, Convert.ToInt32(res));

            return result;
        }

        public static int Update<T>(T entity)
            where T : new()
        {
            return Update(GetTable<T>(), entity);
        }

        private static int Update(DatabaseTable table, Object entity)
        {
            if (table.SuperTable != null) Update(table.SuperTable, entity);

            DatabaseColumn primaryKey = table.Columns.First(x => x.PrimaryKey);

            IEnumerable<DatabaseColumn> valid = table.Columns.Where(x => x != primaryKey);

            String columns = String.Join(",\n  ", valid.Select(x =>
                String.Format("{0} = {1}", x.Name, FormatValue(x.GetValue(entity)))));

            String predicate = String.Format("{0} = {1}", primaryKey.Name, FormatValue(primaryKey.GetValue(entity)));

            StringBuilder builder = new StringBuilder();
            builder.AppendFormat("UPDATE {0} SET\n  {1}\nWHERE {2}",
                table.Name, columns, predicate);

            return ExecuteNonQuery(builder.ToString());
        }

        // TODO: Delete should cascade with supertables
        public static int Delete<T>(T entity)
            where T : new()
        {
            return Delete<T>(new T[] { entity });
        }

        public static int Delete<T>(params Expression<Func<T, bool>>[] predicates)
            where T : new()
        {
            return Delete<T>(Select<T>(predicates));
        }

        public static int Delete<T>(IEnumerable<T> entities)
            where T : new()
        {
            if (!entities.Any()) return 0;

            DatabaseTable table = GetTable<T>();
            DatabaseColumn primaryKey = table.Columns.First(x => x.PrimaryKey);

            String predicate = String.Join("\n  OR ", entities.Select(x => String.Format("{0} = {1}",
                primaryKey.Name, FormatValue(primaryKey.GetValue(x)))));

            StringBuilder builder = new StringBuilder();
            builder.AppendFormat("DELETE FROM {0} WHERE {1}",
                table.Name, predicate);

            if (table.CleanupMethod != null) {
                foreach (var ent in entities) {
                    table.CleanupMethod.Invoke(ent, new Object[0]);
                }
            }

            return ExecuteNonQuery(builder.ToString());
        }

        internal static int DeleteAll(Type t)
        {
            DatabaseTable table = GetTable(t);

            StringBuilder builder = new StringBuilder();
            builder.AppendFormat("DELETE FROM {0} ", table.Name);
            builder.Append("WHERE '1' = '1'");

            if (table.CleanupMethod != null) {
                foreach (var ent in SelectAll(table)) {
                    table.CleanupMethod.Invoke(ent, new Object[0]);
                }
            }

            return ExecuteNonQuery(builder.ToString());
        }

        public static void Disconnect()
        {
            _sConnection.Close();
            _sConnection = null;

            _sTables.Clear();
        }
    }
}
