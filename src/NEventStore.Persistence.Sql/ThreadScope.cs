namespace NEventStore.Persistence.Sql
{
    using System;
    using NEventStore.Logging;

    public class ThreadScope<T> : IDisposable where T : class
    {
        private readonly T _current;
        private readonly ILog _logger = LogFactory.BuildLogger(typeof (ThreadScope<T>));
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, object> _dict = new System.Collections.Concurrent.ConcurrentDictionary<string, object> ();
        private readonly bool _rootScope;
        private readonly string _threadKey;
        private bool _disposed;

        public ThreadScope(string key, Func<T> factory)
        {
            _threadKey = typeof (ThreadScope<T>).Name + ":[{0}]".FormatWith(key ?? string.Empty);

            T parent = Load();
            _rootScope = parent == null;
            _logger.Debug(Messages.OpeningThreadScope, _threadKey, _rootScope);

            _current = parent ?? factory();

            if (_current == null)
            {
                throw new ArgumentException(Messages.BadFactoryResult, "factory");
            }

            if (_rootScope)
            {
                Store(_current);
            }
        }

        public T Current
        {
            get { return _current; }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing || _disposed)
            {
                return;
            }

            _logger.Debug(Messages.DisposingThreadScope, _rootScope);
            _disposed = true;
            if (!_rootScope)
            {
                return;
            }

            _logger.Verbose(Messages.CleaningRootThreadScope);
            Store(null);

            var resource = _current as IDisposable;
            if (resource == null)
            {
                return;
            }

            _logger.Verbose(Messages.DisposingRootThreadScopeResources);
            resource.Dispose();
        }

        private T Load()
        {
            if (!_dict.ContainsKey (_threadKey))
            {
                return null;
            }

            return _dict[_threadKey] as T;
        }

        private void Store(T value)
        {
            if (value != null)
            {
                _dict.AddOrUpdate (_threadKey, value, (key, newValue) => newValue);
            }
            else
            {
                object t;
                _dict.TryRemove (_threadKey, out t);
            }
        }
    }
}