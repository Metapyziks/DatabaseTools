using System;
using System.Linq;
using System.Reflection;
using System.Text;

namespace DatabaseTools
{
    public class DatabaseColumn
    {
        private readonly PropertyInfo _property;

        public String Name { get { return _property.Name; } }
        public Type Type { get { return _property.PropertyType; } }

        public bool NotNull { get; private set; }
        public bool Unique { get; private set; }
        public bool ForeignKey { get; private set; }
        public bool PrimaryKey { get; private set; }
        public bool AutoIncrement { get; private set; }
        public bool FixedLength { get; private set; }

        public DatabaseTable[] ForeignTables { get; private set; }

        public int Capacity { get; private set; }
        public int Capacity2 { get; private set; }

        public DatabaseColumn(PropertyInfo property)
        {
            _property = property;

            NotNull = property.IsDefined<NotNullAttribute>();
            Unique = property.IsDefined<UniqueAttribute>();
            ForeignKey = property.IsDefined<ForeignKeyAttribute>();
            PrimaryKey = property.IsDefined<PrimaryKeyAttribute>();
            AutoIncrement = property.IsDefined<AutoIncrementAttribute>(false);
            FixedLength = property.IsDefined<FixedLengthAttribute>();

            if (property.IsDefined<CapacityAttribute>()) {
                CapacityAttribute val = property.GetCustomAttribute<CapacityAttribute>();
                Capacity = val.Value;
                Capacity2 = val.Value2;
            } else {
                Capacity = 0;
                Capacity2 = 0;
            }
        }

        internal void ResolveForeignKeys(DatabaseTable super)
        {
            if (ForeignKey) {
                ForeignTables = (
                    _property.GetCustomAttributes(typeof(ForeignKeyAttribute), false).SelectMany(x =>
                    ((ForeignKeyAttribute) x).ForeignEntityTypes.Select(y => Database.GetTable(y)))
                ).ToArray();
            }
            if (super != null) {
                if (AutoIncrement) {
                    AutoIncrement = false;
                }
                if (PrimaryKey) {
                    ForeignKey = true;
                    ForeignTables = new[] { super };
                }
            }
        }

        private static String GetSQLTypeName(DatabaseColumn col, Type type)
        {
            if (type.IsEnum)
                return GetSQLTypeName(col, Enum.GetUnderlyingType(type));

            if (type == typeof(String) || type == typeof(Char[])) {
                String name = col.Capacity > 255 ? "NTEXT" :
                    col.FixedLength ? "NCHAR({0})" : "NVARCHAR({0})";
#if LINUX
                name += "COLLATE NOCASE";
#endif
                return name;
            }

            if (type == typeof(Int64) || type == typeof(UInt64) || type == typeof(DateTime))
                return "BIGINT";

            if (type == typeof(Int32) || type == typeof(UInt32))
                return "INTEGER";

            if (type == typeof(Int16) || type == typeof(UInt16))
                return "SMALLINT";

            if (type == typeof(Byte) || type == typeof(SByte))
                return "TINYINT";

            if (type == typeof(Double) || type == typeof(Single))
                return "DECIMAL({0},{1})";

            if (type == typeof(Boolean))
                return "BOOLEAN";

            throw new Exception("Can't find the SQL type of " + type.FullName);
        }

        public String GenerateDefinitionStatement()
        {
            StringBuilder builder = new StringBuilder();

            builder.AppendFormat("{0} {1}", Name, String.Format(
                GetSQLTypeName(this, Type), Capacity, Capacity2));

            if (PrimaryKey)
                builder.Append(" PRIMARY KEY");
            else if (Unique)
                builder.Append(" UNIQUE");
            else if (NotNull)
                builder.Append(" NOT NULL");

            if (AutoIncrement)
#if LINUX
                builder.Append( " AUTOINCREMENT" );
#else
                builder.Append(" IDENTITY");
#endif

            return builder.ToString();
        }

        public object GetValue(object entity)
        {
            object val = _property.GetValue(entity, null);
            if (val is DateTime)
                return ((DateTime) val).Ticks;
            else if (val.GetType().IsEnum)
                return Convert.ChangeType(val, Enum.GetUnderlyingType(val.GetType()));
            else if (val is char[])
                return new String((char[]) val);
            else
                return _property.GetValue(entity, null);
        }

        public void SetValue(object entity, object val)
        {
            if (_property.PropertyType == typeof(DateTime))
                _property.SetValue(entity, new DateTime(Convert.ToInt64(val)), null);
            else if (_property.PropertyType.IsEnum)
                _property.SetValue(entity, Convert.ChangeType(val,
                    Enum.GetUnderlyingType(_property.PropertyType)), null);
            else if (_property.PropertyType == typeof(char[]))
                _property.SetValue(entity, Convert.ToString(val).ToCharArray(), null);
            else
                _property.SetValue(entity, Convert.ChangeType(val, _property.PropertyType), null);
        }

        public override string ToString()
        {
            return GenerateDefinitionStatement();
        }
    }
}
