// Copyright 2013 CaptiveAire Systems
//  
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
// this file except in compliance with the License. You may obtain a copy of the 
// License at 
// 
//     http://www.apache.org/licenses/LICENSE-2.0 
// 
// Unless required by applicable law or agreed to in writing, software distributed 
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
// CONDITIONS OF ANY KIND, either express or implied. See the License for the 
// specific language governing permissions and limitations under the License.

using Magnum.Extensions;
using Magnum.StateMachine;

namespace MassTransit.Persistence.MongoDb
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using MassTransit.Logging;
    using MassTransit.Saga;
    using MassTransit.Util;

    using MongoDB.Bson.Serialization;
    using MongoDB.Driver;
    using MongoDB.Driver.Linq;
    using System.Threading.Tasks;
    using Pipeline;
    public class MongoDbSagaRepository<TSaga> : ISagaRepository<TSaga>, IQuerySagaRepository<TSaga>
        where TSaga : class, ISaga
    {
        private readonly IMongoDatabase _database;

        protected static ILog Log;
        private readonly string _collectionName = typeof(TSaga).ToString();

        /// <summary> Initializes a new instance of the MongoDbSagaRepository class. </summary>
        /// <exception cref="ArgumentNullException"> Thrown when one or more required arguments are null. </exception>
        /// <param name="database"> The database. </param>
        public MongoDbSagaRepository(IMongoDatabase database)
        {
            if (database == null) throw new ArgumentNullException("database");

            if (Log == null) Log = Logger.Get<MongoDbSagaRepository<TSaga>>();

            _database = database;
            RegisterMappings();
        }

        public MongoDbSagaRepository(string connectionString, string databaseName, string collectionName)
        {
            var mongoClient = new MongoClient(connectionString);
            var database = mongoClient.GetDatabase(databaseName);
            _database = database;
            _collectionName = collectionName;

            RegisterMappings();
        }

        /// <summary>
        /// Registers Mongo class maps
        /// </summary>
        private static void RegisterMappings()
        {
            if (!BsonClassMap.IsClassMapRegistered(typeof(TSaga)))
            {
                BsonClassMap.RegisterClassMap<TSaga>(
                    cm =>
                    {
                        cm.AutoMap();
                        cm.MapIdField(s => s.CorrelationId);
                        cm.MapIdMember(s => s.CorrelationId);
                        cm.MapIdProperty(s => s.CorrelationId);
                    });
            }
        }

        /// <summary> Gets the collection. </summary>
        /// <value> The collection. </value>
        protected IMongoCollection<TSaga> Collection => _database.GetCollection<TSaga>(_collectionName);

        public async Task Send<T>(ConsumeContext<T> context, ISagaPolicy<TSaga, T> policy, Pipeline.IPipe<SagaConsumeContext<TSaga, T>> next) where T : class
        {
            if (!context.CorrelationId.HasValue)
                throw new SagaException("The correlation id was not specified", typeof(TSaga), typeof(T));

            var sagaId = context.CorrelationId.Value;
            var inserted = false;
            var collection = Collection;

            TSaga instance;
            if (policy.PreInsertInstance(context, out instance))
            {
                inserted = await PreInsertSagaInstance<T>(collection, instance, context.CancellationToken).ConfigureAwait(false);
            }

            if (instance == null)
            {
                var results = await collection.Find(x => x.CorrelationId == sagaId).ToListAsync().ConfigureAwait(false);
                instance = results.SingleOrDefault();
            }
            else
            {
                if (Log.IsDebugEnabled)
                {
                    Log.DebugFormat($"SAGA:{TypeMetadataCache<TSaga>.ShortName}:{instance.CorrelationId} Used {TypeMetadataCache<T>.ShortName}");
                }

                var sagaConsumeContext = new MongoDbSagaConsumeContext<TSaga, T>(Collection, context, instance);

                await policy.Existing(sagaConsumeContext, next).ConfigureAwait(false);

                await Collection.ReplaceOneAsync(x => x.CorrelationId == instance.CorrelationId, instance).ConfigureAwait(false);
            }
        }

        public async Task SendQuery<T>(SagaQueryConsumeContext<TSaga, T> context, ISagaPolicy<TSaga, T> policy, Pipeline.IPipe<SagaConsumeContext<TSaga, T>> next) where T : class
        {
            try
            {
                var collection = Collection;
                var sagaInstances = await Collection.Find(context.Query.FilterExpression).ToListAsync().ConfigureAwait(false);
                if (sagaInstances.Count == 0)
                {
                    var missingSagaPipe = new MissingPipe<T>(collection, next);
                    await policy.Missing(context, missingSagaPipe).ConfigureAwait(false);
                }
                else
                {
                    foreach(var instance in sagaInstances)
                    {
                        await SendToInstance(context, policy, instance, next).ConfigureAwait(false);
                        await collection.ReplaceOneAsync(x => x.CorrelationId == instance.CorrelationId, instance).ConfigureAwait(false);
                    }
                }
            }
            catch(SagaException ex)
            {
                if (Log.IsErrorEnabled)
                    Log.Error("Saga Exception Occured", ex);
            }
            catch (Exception ex)
            {
                if(Log.IsErrorEnabled)
                    Log.Error($"SAGA:{TypeMetadataCache<TSaga>.ShortName} Exception {TypeMetadataCache<T>.ShortName}", ex);

                throw new SagaException(ex.Message, typeof(TSaga), typeof(T), Guid.Empty, ex);
            }
        }

        public void Probe(ProbeContext context)
        {
            var scope = context.CreateScope("sagaRespository");
            scope.Set(new { Persistence = "mongodb" });
        }

        async Task<IEnumerable<Guid>> IQuerySagaRepository<TSaga>.Find(ISagaQuery<TSaga> query)
        {
            return await Collection.Find(query.FilterExpression)
                .Project(x => x.CorrelationId)
                .ToListAsync()
                .ConfigureAwait(false);
        }

        static async Task<bool> PreInsertSagaInstance<T>(IMongoCollection<TSaga> collection, TSaga instance, System.Threading.CancellationToken cancellationToken)
        {
            try
            {
                await collection.InsertOneAsync(instance, cancellationToken).ConfigureAwait(false);

                Log.DebugFormat($"SAGA:{TypeMetadataCache<TSaga>.ShortName}:{instance.CorrelationId} Used {TypeMetadataCache<T>.ShortName}");

                return true;
            }
            catch(Exception ex)
            {
                if (Log.IsDebugEnabled)
                {
                    Log.DebugFormat($"SAGA:{TypeMetadataCache<TSaga>.ShortName}:{instance.CorrelationId} Dupe {TypeMetadataCache<T>.ShortName} - {ex.Message}");

                }
            }

            return false;
        }

        async Task SendToInstance<T>(SagaQueryConsumeContext<TSaga, T> context, ISagaPolicy<TSaga, T> policy, TSaga instance, 
            IPipe<SagaConsumeContext<TSaga, T>> next)
            where T : class
        {
            try
            {
                if (Log.IsDebugEnabled)
                    Log.DebugFormat($"SAGA:{TypeMetadataCache<TSaga>.ShortName}:{instance.CorrelationId} Used {TypeMetadataCache<T>.ShortName}");

                var sagaConsumeContext = new MongoDbSagaConsumeContext<TSaga, T>(Collection, context, instance);

                await policy.Existing(sagaConsumeContext, next).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new SagaException(ex.Message, typeof(TSaga), typeof(T), instance.CorrelationId, ex);
            }
        }

        class MissingPipe<TMessage> : IPipe<SagaConsumeContext<TSaga, TMessage>> where TMessage : class
        {
            private readonly IMongoCollection<TSaga> _collection;
            readonly IPipe<SagaConsumeContext<TSaga, TMessage>> _next;

            public MissingPipe(IMongoCollection<TSaga> collection, IPipe<SagaConsumeContext<TSaga, TMessage>> next)
            {
                _collection = collection;
                _next = next;
            }

            public void Probe(ProbeContext context)
            {
                _next.Probe(context);
            }

            public async Task Send(SagaConsumeContext<TSaga, TMessage> context)
            {
                if (Log.IsDebugEnabled)
                {
                    Log.DebugFormat($"SAGA:{TypeMetadataCache<TSaga>.ShortName}:{context.Saga.CorrelationId} Added {TypeMetadataCache<TMessage>.ShortName}");
                }

                var proxy = new MongoDbSagaConsumeContext<TSaga, TMessage>(_collection, context, context.Saga);

                await _next.Send(proxy).ConfigureAwait(false);

                if (!proxy.IsCompleted)
                    await _collection.InsertOneAsync(context.Saga).ConfigureAwait(false);
            }
        }

        ///// <summary> Enumerates get saga in this collection. </summary>
        ///// <exception cref="ArgumentNullException"> Thrown when one or more required arguments are null. </exception>
        ///// <exception cref="SagaException"> Thrown when a saga error condition occurs. </exception>
        ///// <typeparam name="TMessage"> Type of the message. </typeparam>
        ///// <param name="context"> The context. </param>
        ///// <param name="sagaId"> Identifier for the saga. </param>
        ///// <param name="selector"> The selector. </param>
        ///// <param name="policy"> The policy. </param>
        ///// <returns> An enumerator that allows foreach to be used to process get saga&lt; t message&gt; in this collection. </returns>
        //public IEnumerable<Action<IConsumeContext<TMessage>>> GetSaga<TMessage>(
        //    IConsumeContext<TMessage> context,
        //    Guid sagaId,
        //    InstanceHandlerSelector<TSaga, TMessage> selector,
        //    ISagaPolicy<TSaga, TMessage> policy) where TMessage : class
        //{
        //    if (context == null) throw new ArgumentNullException("context");
        //    if (selector == null) throw new ArgumentNullException("selector");
        //    if (policy == null) throw new ArgumentNullException("policy");

        //    TSaga instance = Queryable.FirstOrDefault(x => x.CorrelationId == sagaId);

        //    if (instance == null)
        //    {
        //        if (policy.CanCreateInstance(context)) yield return CreateNewSagaAction(sagaId, selector, policy);
        //        else
        //        {
        //            if (Log.IsDebugEnabled)
        //            {
        //                Log.DebugFormat(
        //                    "SAGA: {0} Ignoring Missing {1} for {2}",
        //                    typeof(TSaga).ToFriendlyName(),
        //                    sagaId,
        //                    typeof(TMessage).ToFriendlyName());
        //            }
        //        }
        //    }
        //    else
        //    {
        //        if (policy.CanUseExistingInstance(context)) yield return UseExistingSagaAction(sagaId, selector, policy, instance);
        //        else
        //        {
        //            if (Log.IsDebugEnabled)
        //            {
        //                Log.DebugFormat(
        //                    "SAGA: {0} Ignoring Existing {1} for {2}",
        //                    typeof(TSaga).ToFriendlyName(),
        //                    sagaId,
        //                    typeof(TMessage).ToFriendlyName());
        //            }
        //        }
        //    }
        //}

        ///// <summary> Enumerates select in this collection. </summary>
        ///// <typeparam name="TResult"> Type of the result. </typeparam>
        ///// <param name="transformer"> The transformer. </param>
        ///// <returns> An enumerator that allows foreach to be used to process select&lt; t result&gt; in this collection. </returns>
        //public IEnumerable<TResult> Select<TResult>(Func<TSaga, TResult> transformer)
        //{
        //    return Queryable.Select(transformer).ToList();
        //}

        ///// <summary> Enumerates where in this collection. </summary>
        ///// <param name="filter"> A filter specifying the. </param>
        ///// <returns> An enumerator that allows foreach to be used to process where in this collection. </returns>
        //public IEnumerable<TSaga> Where(ISagaFilter<TSaga> filter)
        //{
        //    return Queryable.Where(filter.FilterExpression).ToList();
        //}

        ///// <summary> Enumerates where in this collection. </summary>
        ///// <typeparam name="TResult"> Type of the result. </typeparam>
        ///// <param name="filter"> A filter specifying the. </param>
        ///// <param name="transformer"> The transformer. </param>
        ///// <returns> An enumerator that allows foreach to be used to process where&lt; t result&gt; in this collection. </returns>
        //public IEnumerable<TResult> Where<TResult>(
        //    ISagaFilter<TSaga> filter,
        //    Func<TSaga, TResult> transformer)
        //{
        //    return Queryable.Where(filter.FilterExpression).Select(transformer).ToList();
        //}

        ///// <summary>
        /////     Gets mongo query.
        ///// </summary>
        ///// <param name="queryable"> The queryable.</param>
        ///// <returns>
        /////     The mongo query.
        ///// </returns>
        //protected IMongoQuery GetMongoQuery(IQueryable<TSaga> queryable)
        //{
        //    if (queryable == null) throw new ArgumentNullException("queryable");

        //    var mongoQueryable = queryable as MongoQueryable<TSaga>;

        //    return mongoQueryable != null ? mongoQueryable.GetMongoQuery() : null;
        //}

        ///// <summary>
        /////     Creates new saga action.
        ///// </summary>
        ///// <exception cref="SagaException"> Thrown when a saga error condition occurs.</exception>
        ///// <typeparam name="TMessage"> Type of the message.</typeparam>
        ///// <param name="sagaId">   Identifier for the saga.</param>
        ///// <param name="selector"> The selector.</param>
        ///// <param name="policy">   The policy.</param>
        ///// <returns>
        /////     The new new saga action&lt; t message&gt;
        ///// </returns>
        //Action<IConsumeContext<TMessage>> CreateNewSagaAction<TMessage>(
        //    Guid sagaId,
        //    InstanceHandlerSelector<TSaga, TMessage> selector,
        //    ISagaPolicy<TSaga, TMessage> policy)
        //    where TMessage : class
        //{
        //    return x =>
        //    {
        //        if (Log.IsDebugEnabled)
        //        {
        //            Log.DebugFormat(
        //                "SAGA: {0} Creating New {1} for {2}",
        //                typeof(TSaga).ToFriendlyName(),
        //                sagaId,
        //                typeof(TMessage).ToFriendlyName());
        //        }

        //        try
        //        {
        //            TSaga instance = policy.CreateInstance(x, sagaId);

        //            foreach (var callback in selector(instance, x))
        //            {
        //                callback(x);
        //            }

        //            if (!policy.CanRemoveInstance(instance)) Collection.Insert(instance, WriteConcern.Acknowledged);
        //            else Collection.Save(instance, WriteConcern.Acknowledged);
        //        }
        //        catch (Exception ex)
        //        {
        //            var sagaException = new SagaException(
        //                "Create Saga Instance Exception",
        //                typeof(TSaga),
        //                typeof(TMessage),
        //                sagaId,
        //                ex);

        //            if (Log.IsErrorEnabled) Log.Error(sagaException);

        //            throw sagaException;
        //        }
        //    };
        //}

        ///// <summary>
        /////     Use existing saga action.
        ///// </summary>
        ///// <exception cref="SagaException"> Thrown when a saga error condition occurs.</exception>
        ///// <typeparam name="TMessage"> Type of the message.</typeparam>
        ///// <param name="sagaId">   Identifier for the saga.</param>
        ///// <param name="selector"> The selector.</param>
        ///// <param name="policy">   The policy.</param>
        ///// <param name="instance"> The instance.</param>
        ///// <returns>
        /////     .
        ///// </returns>
        //Action<IConsumeContext<TMessage>> UseExistingSagaAction<TMessage>(
        //    Guid sagaId,
        //    InstanceHandlerSelector<TSaga, TMessage> selector,
        //    ISagaPolicy<TSaga, TMessage> policy,
        //    TSaga instance)
        //    where TMessage : class
        //{
        //    return x =>
        //    {
        //        if (Log.IsDebugEnabled)
        //        {
        //            Log.DebugFormat(
        //                "SAGA: {0} Using Existing {1} for {2}",
        //                typeof(TSaga).ToFriendlyName(),
        //                sagaId,
        //                typeof(TMessage).ToFriendlyName());
        //        }

        //        try
        //        {
        //            foreach (var callback in selector(instance, x))
        //            {
        //                callback(x);
        //            }

        //            if (policy.CanRemoveInstance(instance))
        //            {
        //                Collection.Remove(
        //                    GetMongoQuery(Queryable.Where(q => q.CorrelationId == sagaId)),
        //                    RemoveFlags.Single,
        //                    WriteConcern.Acknowledged);
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            var sagaException = new SagaException(
        //                "Existing Saga Instance Exception",
        //                typeof(TSaga),
        //                typeof(TMessage),
        //                sagaId,
        //                ex);
        //            if (Log.IsErrorEnabled) Log.Error(sagaException);

        //            throw sagaException;
        //        }
        //    };
        //}
    }
}