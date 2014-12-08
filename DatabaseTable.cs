using System;
using System.Linq;
using System.Reflection;
using System.Text;

namespace DatabaseTools
{
#if SQLITE
    namespace Sqlite
    {
#elif SQL_SERVER_CE
    namespace SqlServerCe
    {
#elif SQL_CLIENT
    namespace SqlClient
    {
#endif
        public class DatabaseTable
        {
            private readonly Type _type;

            public Type Type
            {
                get { return _type; }
            }

            public String Name
            {
                get { return _type.Name; }
            }

            public bool DropOnConnect { get; private set; }

            public DatabaseTable SuperTable { get; private set; }
            public DatabaseColumn[] Columns { get; private set; }

            public MethodInfo CleanupMethod { get; private set; }

            public DatabaseTable(Type type)
            {
                _type = type;

                var attrib = type.GetCustomAttribute<DatabaseEntityAttribute>();
                if (attrib != null) DropOnConnect = attrib.DropOnConnect;
            }

            public bool ShouldInclude(PropertyInfo property)
            {
                if (!property.IsDefined<ColumnAttribute>()) return false;

                if (property.IsDefined<PrimaryKeyAttribute>()) return true;

                var super = _type.BaseType;
                while (super.IsDefined<DatabaseEntityAttribute>()) {
                    if (super.GetProperty(property.Name) != null) {
                        return false;
                    }
                    super = super.BaseType;
                }

                return true;
            }

            internal void BuildColumns()
            {
                var count = _type.GetProperties().Count(ShouldInclude);
                Columns = new DatabaseColumn[count];

                var i = 0;
                foreach (var property in _type.GetProperties()) {
                    if (ShouldInclude(property)) {
                        Columns[i++] = new DatabaseColumn(property);
                    }
                }

                foreach (var method in _type.GetMethods()) {
                    if (!method.IsDefined<CleanUpMethodAttribute>() || method.GetParameters().Length != 0) continue;

                    CleanupMethod = method;
                    break;
                }
            }

            internal void ResolveForeignKeys()
            {
                if (_type.BaseType.IsDefined<DatabaseEntityAttribute>()) {
                    SuperTable = Database.GetTable(_type.BaseType);
                }

                foreach (var col in Columns) {
                    col.ResolveForeignKeys(SuperTable);
                }
            }

            public String GenerateDefinitionStatement()
            {
                var builder = new StringBuilder();
                builder.AppendFormat("CREATE TABLE {0}\n(\n", Name);
                for (var i = 0; i < Columns.Length; ++i) {
                    builder.AppendFormat("  {0}{1}\n", Columns[i].GenerateDefinitionStatement(),
                        i < Columns.Length - 1 ? "," : "");
                }
                builder.AppendFormat(");\n");
                return builder.ToString();
            }

            public void Drop()
            {
                Database.Log("  Dropping table {0}...", Name);
                Database.ExecuteNonQuery("DROP TABLE {0}", Name);
            }

            public void Create()
            {
                Database.Log("  Creating table {0}...", Name);
                Database.ExecuteNonQuery(GenerateDefinitionStatement());
            }

            public override string ToString()
            {
                return Name;
            }
        }
    }
}
