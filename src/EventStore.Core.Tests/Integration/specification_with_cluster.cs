using System;
using System.Net;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using EventStore.Core.Tests.Helpers;
using NUnit.Framework;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace EventStore.Core.Tests.Integration {
	// ReSharper disable once InconsistentNaming
	public class specification_with_cluster : SpecificationWithDirectoryPerTestFixture {
		protected MiniClusterNode[] Nodes = new MiniClusterNode[3];
		protected Endpoints[] NodeEndpoints = new Endpoints[3];
		protected IEventStoreConnection Conn;
		protected UserCredentials Admin = DefaultData.AdminCredentials;

		protected Dictionary<int, Func<bool, MiniClusterNode>> NodeCreationFactory =
			new Dictionary<int, Func<bool, MiniClusterNode>>();


		protected class Endpoints {
			public readonly IPEndPoint InternalTcp;
			public readonly IPEndPoint InternalTcpSec;
			public readonly IPEndPoint InternalHttp;
			public readonly IPEndPoint ExternalTcp;
			public readonly IPEndPoint ExternalTcpSec;
			public readonly IPEndPoint ExternalHttp;

			public IEnumerable<int> Ports() {
				yield return InternalTcp.Port;
				yield return InternalTcpSec.Port;
				yield return InternalHttp.Port;
				yield return ExternalTcp.Port;
				yield return ExternalTcpSec.Port;
				yield return ExternalHttp.Port;
			}

			private readonly List<Socket> _sockets;

			public Endpoints() {
				_sockets = new List<Socket>();

				var defaultLoopBack = new IPEndPoint(IPAddress.Loopback, 0);

				var internalTcp = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
				internalTcp.Bind(defaultLoopBack);
				_sockets.Add(internalTcp);

				var internalTcpSecure =
					new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
				internalTcpSecure.Bind(defaultLoopBack);
				_sockets.Add(internalTcpSecure);

				var internalHttp = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
				internalHttp.Bind(defaultLoopBack);
				_sockets.Add(internalHttp);

				var externalTcp = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
				externalTcp.Bind(defaultLoopBack);
				_sockets.Add(externalTcp);

				var externalTcpSecure =
					new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
				externalTcpSecure.Bind(defaultLoopBack);
				_sockets.Add(externalTcpSecure);

				var externalHttp = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
				externalHttp.Bind(defaultLoopBack);
				_sockets.Add(externalHttp);

				InternalTcp = CopyEndpoint((IPEndPoint)internalTcp.LocalEndPoint);
				InternalTcpSec = CopyEndpoint((IPEndPoint)internalTcpSecure.LocalEndPoint);
				InternalHttp = CopyEndpoint((IPEndPoint)internalHttp.LocalEndPoint);
				ExternalTcp = CopyEndpoint((IPEndPoint)externalTcp.LocalEndPoint);
				ExternalTcpSec = CopyEndpoint((IPEndPoint)externalTcpSecure.LocalEndPoint);
				ExternalHttp = CopyEndpoint((IPEndPoint)externalHttp.LocalEndPoint);
			}

			public void DisposeSockets() {
				foreach (var socket in _sockets) {
					socket.Dispose();
				}
			}

			private IPEndPoint CopyEndpoint(IPEndPoint endpoint) {
				return new IPEndPoint(endpoint.Address, endpoint.Port);
			}
		}

		[OneTimeSetUp]
		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();

			NodeEndpoints[0] = new Endpoints();
			NodeEndpoints[1] = new Endpoints();
			NodeEndpoints[2] = new Endpoints();

			NodeEndpoints[0].DisposeSockets();
			NodeEndpoints[1].DisposeSockets();
			NodeEndpoints[2].DisposeSockets();

			var duplicates = NodeEndpoints[0].Ports().Concat(NodeEndpoints[1].Ports())
				.Concat(NodeEndpoints[2].Ports())
				.GroupBy(x => x)
				.Where(g => g.Count() > 1)
				.Select(x => x.Key)
				.ToList();

			Assert.IsEmpty(duplicates);

			NodeCreationFactory.Add(0, wait => CreateNode(0,
				NodeEndpoints[0], new[] {NodeEndpoints[1].InternalHttp, NodeEndpoints[2].InternalHttp},
				wait));
			NodeCreationFactory.Add(1, wait => CreateNode(1,
				NodeEndpoints[1], new[] {NodeEndpoints[0].InternalHttp, NodeEndpoints[2].InternalHttp},
				wait));
			NodeCreationFactory.Add(2, wait => CreateNode(2,
				NodeEndpoints[2], new[] {NodeEndpoints[0].InternalHttp, NodeEndpoints[1].InternalHttp},
				wait));

			Nodes[0] = NodeCreationFactory[0](true);
			Nodes[1] = NodeCreationFactory[1](true);
			Nodes[2] = NodeCreationFactory[2](true);

			BeforeNodesStart();

			Nodes[0].Start();
			Nodes[1].Start();
			Nodes[2].Start();

			await Task.WhenAll(Nodes.Select(x => x.Started)).WithTimeout(TimeSpan.FromSeconds(30));

			Conn = CreateConnection();
			await Conn.ConnectAsync();

			await Given();
		}

		protected virtual IEventStoreConnection CreateConnection() {
			return EventStoreConnection.Create(Nodes[0].ExternalTcpEndPoint);
		}

		protected virtual void BeforeNodesStart() {
		}

		protected virtual Task Given() => Task.CompletedTask;

		protected Task ShutdownNode(int nodeNum) {
			return Nodes[nodeNum].Shutdown(keepDb: true);
		}

		protected virtual MiniClusterNode CreateNode(int index, Endpoints endpoints, IPEndPoint[] gossipSeeds,
			bool wait = true) {
			var node = new MiniClusterNode(
				PathName, index, endpoints.InternalTcp, endpoints.InternalTcpSec, endpoints.InternalHttp,
				endpoints.ExternalTcp,
				endpoints.ExternalTcpSec, endpoints.ExternalHttp, skipInitializeStandardUsersCheck: false,
				subsystems: new ISubsystem[] { }, gossipSeeds: gossipSeeds, inMemDb: false);
			return node;
		}

		[OneTimeTearDown]
		public override async Task TestFixtureTearDown() {
			Conn.Close();
			await Task.WhenAll(
				Nodes[0].Shutdown(),
				Nodes[1].Shutdown(),
				Nodes[2].Shutdown());

			await base.TestFixtureTearDown();
		}

		protected static void WaitIdle() {
		}

		protected MiniClusterNode GetMaster() {
			return Nodes.First(x => x.NodeState == Data.VNodeState.Master);
		}

		protected MiniClusterNode[] GetReplicas() {
			return Nodes.Where(x => x.NodeState != Data.VNodeState.Master).ToArray();
		}
	}
}
