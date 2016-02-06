using MassTransit.Context;
using MassTransit.Logging;
using MassTransit.Saga;
using MassTransit.Util;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MassTransit.Persistence.MongoDb
{
    internal class MongoDbSagaConsumeContext<TSaga, TMessage> : ConsumeContextProxyScope<TMessage>, SagaConsumeContext<TSaga, TMessage>
        where TMessage : class where TSaga : class, ISaga
    {
        private readonly IMongoCollection<TSaga> _collection;
        static readonly ILog Log = Logger.Get<MongoDbSagaRepository<TSaga>>();

        public MongoDbSagaConsumeContext(IMongoCollection<TSaga> collection, ConsumeContext<TMessage> context, TSaga instance)
        {
            _collection = collection;
            Saga = instance;
        }

        public bool IsCompleted { get; private set; }

        public TSaga Saga { get; }

        public SagaConsumeContext<TSaga, T> PopContext<T>() where T : class
        {
            var context = this as SagaConsumeContext<TSaga, T>;
            if (context == null)
                throw new ContextException($"The ConsumeContext<{TypeMetadataCache<TMessage>.ShortName} could not be cast to {TypeMetadataCache<T>.ShortName}");

            return context;
        }

        public async Task SetCompleted()
        {
            await _collection.DeleteOneAsync(x => x.CorrelationId == Saga.CorrelationId, CancellationToken).ConfigureAwait(false);

            IsCompleted = true;
            if (Log.IsDebugEnabled)
                Log.DebugFormat($"SAGA:{TypeMetadataCache<TSaga>.ShortName}:{TypeMetadataCache<TMessage>.ShortName} Removed {Saga.CorrelationId}");
        }
    }
}
