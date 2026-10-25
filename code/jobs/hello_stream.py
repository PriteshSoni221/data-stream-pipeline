from pyflink.datastream import StreamExecutionEnvironment


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    (
        env.from_collection(["hello", "from", "pyflink", "on", "docker"])
        .map(lambda x: f"event={x}")
        .print()
    )

    env.execute("hello-pyflink-stream")


if __name__ == "__main__":
    main()
