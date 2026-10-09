import pytest

import kbus


class Billing:
    def __init__(self):
        self.calls = []

    @kbus.expose
    async def total(self, order: int) -> float:
        self.calls.append(order)
        return order * 1.5

    @kbus.expose
    async def names(self) -> list:
        return ["a", "b"]

    @kbus.expose
    async def divide(self, by: int) -> float:
        return 1 / by

    async def secret(self) -> str:
        return "hidden"


@pytest.fixture
async def billing(members):
    obj = Billing()
    await members("billing", kbus.expose(obj))
    return obj


async def test_exposed_method_round_trip(billing, members):
    shop = await members("shop")
    assert await shop.route("billing").total(order=4) == 6.0
    assert await shop.route("billing").names() == ["a", "b"]
    assert billing.calls == [4]


async def test_unmarked_attribute_is_no_such_route(billing, members):
    shop = await members("shop")
    with pytest.raises(kbus.NoSuchRoute):
        await shop.route("billing").secret()
    with pytest.raises(kbus.NoSuchRoute):
        await shop.route("billing").calls()


async def test_exception_in_method_is_remote_error(billing, members):
    shop = await members("shop")
    with pytest.raises(kbus.RemoteError) as info:
        await shop.route("billing").divide(by=0)
    assert info.value.type == "ZeroDivisionError"
    assert "division by zero" in info.value.message
    assert "ZeroDivisionError" in info.value.traceback


async def test_positional_arguments_are_refused_locally(billing, members):
    shop = await members("shop")
    with pytest.raises(TypeError):
        await shop.route("billing").total(4)


async def test_missing_member_through_proxy(members):
    shop = await members("shop")
    with pytest.raises(kbus.NoSuchMember):
        await shop.route("nobody").total(order=1)
