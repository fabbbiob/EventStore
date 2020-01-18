using System;
using System.Collections.Generic;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.DataStructures;
using EventStore.Core.Index;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.LogRecords;
using NUnit.Framework;
// ReSharper disable InconsistentNaming

namespace EventStore.Core.Tests.Services.Storage {
	[TestFixture]
	public abstract class write_events_to_index_scenario {
		protected InMemoryBus Publisher;
		protected ITransactionFileReader TFReader;
		protected ITableIndex TableIndex;
		protected IIndexBackend IndexBackend;
		protected IIndexReader IndexReader;
		protected IIndexWriter IndexWriter;
		protected IIndexCommitter IndexCommitter;
		protected ObjectPool<ITransactionFileReader> ReaderPool;
		protected const int RecordOffset = 1000;
		public IList<PrepareLogRecord> CreatePrepareLogRecord(string stream, int expectedVersion, string eventType, Guid eventId, long transactionPosition) {
			return new []{
				new PrepareLogRecord (
					transactionPosition,
					Guid.NewGuid(),
					eventId,
					transactionPosition,
					0,
					stream,
					expectedVersion,
					DateTime.Now,
					PrepareFlags.SingleWrite | PrepareFlags.IsCommitted,
					eventType,
					new byte[0],
					new byte[0]
				)
			};
		}

		public IList<PrepareLogRecord> CreatePrepareLogRecords(string stream, int expectedVersion, IList<string> eventTypes, IList<Guid> eventIds, long transactionPosition) {
			if (eventIds.Count != eventTypes.Count)
				throw new Exception("eventType and eventIds length mismatch!");
			if (eventIds.Count == 0)
				throw new Exception("eventIds is empty");
			if (eventIds.Count == 1)
				return CreatePrepareLogRecord(stream, expectedVersion, eventTypes[0], eventIds[0], transactionPosition);

			var numEvents = eventTypes.Count;

			var prepares = new List<PrepareLogRecord>();
			for (var i = 0; i < numEvents; i++) {
				PrepareFlags flags = PrepareFlags.Data | PrepareFlags.IsCommitted;
				if (i == 0)
					flags |= PrepareFlags.TransactionBegin;
				if (i == numEvents - 1)
					flags |= PrepareFlags.TransactionEnd;

				prepares.Add(
					new PrepareLogRecord(
						transactionPosition + RecordOffset * i,
						Guid.NewGuid(),
						eventIds[i],
						transactionPosition,
						i,
						stream,
						expectedVersion + i,
						DateTime.Now,
						flags,
						eventTypes[i],
						new byte[0],
						new byte[0]
					)
				);
			}

			return prepares;
		}

		public CommitLogRecord CreateCommitLogRecord(long logPosition, long transactionPosition, long firstEventNumber) {
			return new CommitLogRecord(logPosition, Guid.NewGuid(), transactionPosition, DateTime.Now, 0);
		}

		public void WriteToDB(IList<PrepareLogRecord> prepares) {
			foreach (var prepare in prepares) {
				((FakeInMemoryTfReader)TFReader).AddRecord(prepare, prepare.LogPosition);
			}
		}

		public void WriteToDB(CommitLogRecord commit) {
			((FakeInMemoryTfReader)TFReader).AddRecord(commit, commit.LogPosition);
		}

		public void PreCommitToIndex(IList<PrepareLogRecord> prepares) {
			IndexWriter.PreCommit(prepares);
		}

		public void PreCommitToIndex(CommitLogRecord commitLogRecord) {
			IndexWriter.PreCommit(commitLogRecord);
		}

		public void CommitToIndex(IList<PrepareLogRecord> prepares) {
			IndexCommitter.Commit(prepares, false, false);
		}

		public void CommitToIndex(CommitLogRecord commitLogRecord) {
			IndexCommitter.Commit(commitLogRecord, false, false);
		}

		public abstract void WriteEvents();

		[OneTimeSetUp]
		public virtual void TestFixtureSetUp() {
			Publisher = new InMemoryBus("publisher");
			TFReader = new FakeInMemoryTfReader(RecordOffset);
			TableIndex = new FakeInMemoryTableIndex();
			ReaderPool = new ObjectPool<ITransactionFileReader>(
				"ReadIndex readers pool", 5, 100,
				() => TFReader);
			IndexBackend = new IndexBackend(ReaderPool, 100000, 100000);
			IndexReader = new IndexReader(IndexBackend, TableIndex, new StreamMetadata(maxCount: 100000), 100, false);
			IndexWriter = new IndexWriter(IndexBackend, IndexReader);
			IndexCommitter = new Core.Services.Storage.ReaderIndex.IndexCommitter(Publisher, IndexBackend, IndexReader, TableIndex, new InMemoryCheckpoint(-1), false);

			WriteEvents();
		}

		[OneTimeTearDown]
		public virtual void TestFixtureTearDown() {
			ReaderPool.Dispose();
		}
	}
}
