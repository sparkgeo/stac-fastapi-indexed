from asyncio import AbstractEventLoop, get_event_loop, new_event_loop, set_event_loop


def get_callable_event_loop() -> AbstractEventLoop:
    loop = get_event_loop()
    loop_existed = loop.is_running()
    if not loop_existed:
        loop = new_event_loop()
        set_event_loop(loop)
    return loop
