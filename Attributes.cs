using System;

namespace DatabaseTools
{
    [AttributeUsage(AttributeTargets.Class)]
    public class DatabaseEntityAttribute : Attribute
    {
        public bool DropOnConnect { get; set; }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnAttribute : Attribute { }

    [AttributeUsage(AttributeTargets.Property)]
    public class NotNullAttribute : ColumnAttribute { }

    [AttributeUsage(AttributeTargets.Property)]
    public class UniqueAttribute : NotNullAttribute { }

    [AttributeUsage(AttributeTargets.Property)]
    public class ForeignKeyAttribute : ColumnAttribute
    {
        public readonly Type[] ForeignEntityTypes;

        public ForeignKeyAttribute(params Type[] foreignEntityTypes)
        {
            ForeignEntityTypes = foreignEntityTypes;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class PrimaryKeyAttribute : UniqueAttribute { }

    [AttributeUsage(AttributeTargets.Property)]
    public class AutoIncrementAttribute : ColumnAttribute { }

    [AttributeUsage(AttributeTargets.Property)]
    public class FixedLengthAttribute : ColumnAttribute { }

    [AttributeUsage(AttributeTargets.Property)]
    public class CapacityAttribute : ColumnAttribute
    {
        public readonly int Value;
        public readonly int Value2;

        public CapacityAttribute(int value, int value2 = 0)
        {
            Value = value;
            Value2 = value2;
        }
    }

    [AttributeUsage(AttributeTargets.Method)]
    public class CleanUpMethodAttribute : Attribute { }
}
