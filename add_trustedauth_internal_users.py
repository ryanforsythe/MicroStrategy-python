#Adds trusted authentication to users. Uses mstrio-py and Workstation. Helps transition from standard or Windows Auth to SAML authentication.
from mstrio.connection import get_connection
from mstrio.users_and_groups import (
    create_users_from_csv, list_user_groups, list_users, User, UserGroup
)
from typing import List

conn = get_connection(workstationData)

def add_trustedauth_internal_users(
    connection: "Connection", domain="microstrategy.com"
) -> List["User"]:
    """Add email address with a form `{username}@microstrategy.com`
    to every user which is enabled but doesn't have an email address.
    For each successfully added email address a message will be printed.

    Args:
        connection: MicroStrategy connection object returned by
            `connection.Connection()`
        domain: name of the domain in the email address (it should be
            provided without '@' symbol). Default value is "microstrategy.com".

    Returns:
        list of users to which email addresses where added
    """
    # get all users that are enabled
    all_users = list_users(connection=connection)
    users_ = [u for u in all_users if u.enabled]
    modified_users_ = []
    for user_ in users_:
        # add email address only for those users which don't have one
        if not user_.trust_id and 'microstrategy.com' in user_.username:
            email_address = user_.username
            user_.alter(trust_id=email_address)
            modified_users_.append(user_)

    return modified_users_


# execute adding email to new users
# if needed provide some domain - default is 'microstrategy.com'
add_trustedauth_internal_users(conn)