namespace NEventStore.Persistence.Sql.SqlDialects
{
    using System;
    using System.Reflection;

    public class SqliteDialect : CommonSqlDialect
    {
        public override string InitializeStorage
        {
            get { return SqliteStatements.InitializeStorage; }
        }

        // Sqlite wants all parameters to be a part of the query
        public override string GetCommitsFromStartingRevision
        {
            get { return base.GetCommitsFromStartingRevision.Replace("\n ORDER BY ", "\n  AND @Skip = @Skip\nORDER BY "); }
        }

        public override string PersistCommit
        {
            get { return SqliteStatements.PersistCommit; }
        }

        public override bool IsDuplicate(Exception exception)
        {
            string message = exception.Message.ToUpperInvariant();
            return message.Contains("DUPLICATE") || message.Contains("UNIQUE") || message.Contains("CONSTRAINT");

        }

        public override DateTime ToDateTime(object value)
        {
            if (value.GetType ().GetTypeInfo ().Name == "String")
            {
                return DateTime.Parse (value.ToString ()).ToUniversalTime ();
            }
            else
            {
                return ((DateTime)value).ToUniversalTime ();
            }
        }
    }
}