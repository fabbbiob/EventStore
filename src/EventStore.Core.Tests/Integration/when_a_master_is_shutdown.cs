using EventStore.Core.Bus;
using EventStore.Core.Messages;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace EventStore.Core.Tests.Integration {
	[TestFixture, Category("LongRunning"), Ignore("Flaky test - e.g. if multiple elections take place")]
	// ReSharper disable once InconsistentNaming
	public class when_a_master_is_shutdown : specification_with_cluster {
		private readonly List<Guid> _epochIds = new List<Guid>();
		private readonly List<string> _roleAssignments = new List<string>();
		private CountdownEvent _expectedNumberOfEvents;
		private readonly object _lock = new object();

		protected override void BeforeNodesStart() {
			Nodes.ToList().ForEach(x => {
				x.Node.MainBus.Subscribe(new AdHocHandler<SystemMessage.BecomeMaster>(Handle));
				x.Node.MainBus.Subscribe(new AdHocHandler<SystemMessage.BecomeSlave>(Handle));
				x.Node.MainBus.Subscribe(new AdHocHandler<SystemMessage.EpochWritten>(Handle));
			});

			_expectedNumberOfEvents = new CountdownEvent(3 /*role assignments*/ + 1 /*epoch write*/);
			base.BeforeNodesStart();
		}

		protected override async Task Given() {
			_expectedNumberOfEvents.Wait(5000);
			var master = Nodes.First(x => x.NodeState == Data.VNodeState.Master);
			await ShutdownNode(master.DebugIndex);
			_expectedNumberOfEvents = new CountdownEvent(2 /*role assignments*/ + 1 /*epoch write*/);
			_expectedNumberOfEvents.Wait(5000);
			await base.Given();
		}

		private void Handle(SystemMessage.BecomeMaster msg) {
			lock (_lock) {
				_roleAssignments.Add("master");
			}

			_expectedNumberOfEvents?.Signal();
		}

		private void Handle(SystemMessage.BecomeSlave msg) {
			lock (_lock) {
				_roleAssignments.Add("slave");
			}

			_expectedNumberOfEvents?.Signal();
		}

		private void Handle(SystemMessage.EpochWritten msg) {
			lock (_lock) {
				_epochIds.Add(msg.Epoch.EpochId);
			}

			_expectedNumberOfEvents?.Signal();
		}

		[Test]
		public void should_assign_master_and_slave_roles_correctly() {
			Assert.AreEqual(5, _roleAssignments.Count);

			Assert.AreEqual(1, _roleAssignments.Take(3).Count(x => x.Equals("master")));
			Assert.AreEqual(2, _roleAssignments.Take(3).Count(x => x.Equals("slave")));

			//after shutting down
			Assert.AreEqual(1, _roleAssignments.Skip(3).Take(2).Count(x => x.Equals("master")));
			Assert.AreEqual(1, _roleAssignments.Skip(3).Take(2).Count(x => x.Equals("slave")));
		}

		[Test]
		public void should_have_two_unique_epoch_writes() {
			Assert.AreEqual(2, _epochIds.Distinct().Count());
		}
	}
}
