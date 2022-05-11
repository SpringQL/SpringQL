# Module stream_engine

Stream Engine component.

Responsible for pipeline management and execution.

Stream engine has 2 executors:

1. SQL executor
2. Autonomous executor

SQL executor receives commands from user interface to quickly change some status of a pipeline.
It does not deal with stream data (Row, to be precise).

Autonomous executor deals with stream data.

Both SQL executor and autonomous executor instance run at a main thread, while autonomous executor has workers which run at different worker threads.

## Entities inside Stream Engine

![Entities inside Stream Engine](https://raw.githubusercontent.com/SpringQL/SpringQL.github.io/main/static/img/stream-engine-architecture-entity.svg)

## Communication between entities

Workers in AutonomousExecutor interact via EventQueue (Choreography-based Saga pattern).

![Communication between entities](https://raw.githubusercontent.com/SpringQL/SpringQL.github.io/main/static/img/stream-engine-architecture-communication.svg)
