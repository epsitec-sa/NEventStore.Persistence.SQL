using NEventStore.Persistence.Sql.SqlDialects;
using System;
using System.Linq;
using Xunit;

namespace NEventStore.Core.Tests
{
    public class EventStore
    {
        [Fact]
        public void Build()
        {
            var connectionString = string.Format (System.Globalization.CultureInfo.InvariantCulture, "Data Source={0}", "mydatabase.db");

            var eventsStore = Wireup.Init ()
                                     .UsingSqlPersistence ("EventStore", "Microsoft.Data.SQLite", connectionString)
                                     .WithDialect (new SqliteDialect ())
                                     .InitializeStorageEngine ()
                                     .Build ();
        }

        [Fact]
        public void OpenStream()
        {
            var connectionString = string.Format (System.Globalization.CultureInfo.InvariantCulture, "Data Source={0}", "mydatabase.db");

            var eventsStore = Wireup.Init ()
                                     .UsingSqlPersistence ("EventStore", "Microsoft.Data.SQLite", connectionString)
                                     .WithDialect (new SqliteDialect ())
                                     .InitializeStorageEngine ()
                                     .Build ();

            var eventsStream = eventsStore.OpenStream ("Banking", "1234", 0, int.MaxValue);
        }


        [Fact]
        public void AddEventsAndCommit()
        {
            var connectionString = string.Format (System.Globalization.CultureInfo.InvariantCulture, "Data Source={0}", "mydatabase.db");

            var eventsStore = Wireup.Init ()
                                     .UsingSqlPersistence ("EventStore", "Microsoft.Data.SQLite", connectionString)
                                     .WithDialect (new SqliteDialect ())
                                     .InitializeStorageEngine ()
                                     .Build ();

            var eventsStream = eventsStore.OpenStream ("Banking", "1234", 0, int.MaxValue);


            eventsStream.Add (
                new EventMessage
                {
                    Body = "hello"
                }
            );


            eventsStream.CommitChanges (System.Guid.NewGuid ());
        }


        [Fact]
        public void RetrieveEvents()
        {
            var connectionString = string.Format (System.Globalization.CultureInfo.InvariantCulture, "Data Source={0}", "mydatabase.db");

            var eventsStore = Wireup.Init ()
                                     .UsingSqlPersistence ("EventStore", "Microsoft.Data.SQLite", connectionString)
                                     .WithDialect (new SqliteDialect ())
                                     .InitializeStorageEngine ()
                                     .Build ();

            var eventsStream = eventsStore.OpenStream ("Banking", "1234", 0, int.MaxValue);


            var commits = eventsStore.Advanced
                .GetFrom ().ToList ();
        }
    }
}
