from decimal import Decimal, ROUND_DOWN


def calculate_purchase_points(amount: Decimal) -> int:
    """Начисляет 5% от суммы чека, отбрасывая дробную часть."""
    return int(
        (amount * Decimal("0.05")).to_integral_value(rounding=ROUND_DOWN)
    )


def validate_operation(
    operation_type: str,
    amount: Decimal | None,
    points: int | None,
    balance: Decimal,
    purchase: dict | None = None,
    already_returned: bool = False
) -> tuple[str | None, int]:
    """
    Проверяет бизнес-условия операции.
    Возвращает причину отказа и изменение баланса в баллах.
    """

    if operation_type == "purchase":
        earned = calculate_purchase_points(amount)
        return None, earned

    if operation_type == "return":
        if purchase is None:
            return "purchase_not_found", 0

        if already_returned:
            return "already_returned", 0

        return None, -int(purchase["points_delta"])

    if operation_type == "redeem":
        max_points = int(
            (amount * Decimal("0.30")).to_integral_value(rounding=ROUND_DOWN)
        )

        if points > max_points:
            return "limit_exceeded", 0

        if Decimal(points) > balance:
            return "insufficient_balance", 0

        return None, -points

    return "unknown_operation_type", 0
