from dr_exec import hello


def test_hello() -> None:
    assert hello() == "Hello from dr-exec!"
