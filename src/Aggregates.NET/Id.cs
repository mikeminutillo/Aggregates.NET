﻿using System;

namespace Aggregates
{
    public class Id : IEquatable<Id>
    {
        internal object Value { get; set; }

        public Id(string id) { Value = id; }
        public Id(long id) { Value = id; }
        public Id(Guid id) { Value = id; }

        // provides a hook for testing
        protected virtual long GetLongValue()
        {
            return (long)Value;
        }
        protected virtual Guid GetGuidValue()
        {
            return (Guid)Value;
        }
        protected virtual string GetStringValue()
        {
            return (string)Value;
        }

        public static implicit operator Id(string id) => new Id(id);
        public static implicit operator Id(long id) => new Id(id);
        public static implicit operator Id(Guid id) => new Id(id);
    
        public static implicit operator long(Id id) => id.GetLongValue();
        public static implicit operator Guid(Id id) => id.GetGuidValue();
        public static implicit operator string(Id id) => id?.GetStringValue();
        public static implicit operator long?(Id id) => id?.GetLongValue();
        public static implicit operator Guid?(Id id) => id?.GetGuidValue();

        public override string ToString()
        {
            return Value.ToString();
        }

        public bool Equals(Id other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(Value, other.Value);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return obj.GetType() == this.GetType() && Equals((Id)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Value?.GetHashCode() ?? 0) * 397);
            }
        }

        public static bool operator ==(Id left, Id right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(Id left, Id right)
        {
            return !Equals(left, right);
        }
    }
}
