import ipaddress


def is_blocked_ip(ip_str: str) -> bool:
    """True if an IP is private, loopback, link-local, reserved, etc.

    These are the ranges a crawler that follows untrusted links must not reach —
    internal services and cloud metadata endpoints (169.254.169.254) live here.
    """
    try:
        ip = ipaddress.ip_address(ip_str)
    except ValueError:
        return False
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    )
