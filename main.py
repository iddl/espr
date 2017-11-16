import argparse
import json
import sys

# LOL @ THIS CODE


class ParseException(Exception):
    def __init__(self, message):
        self.message = message

    def __repr__(self):
        print(self.message)


def parse_stdin(data):
    try:
        stdin = json.loads(data)
    except ValueError:
        raise ParseException('Unable to parse JSON')

    try:
        by_shard = stdin['profile']['shards']
    except KeyError:
        raise ParseException('stdin does not contain profile info')

    return by_shard


def visit_tree(head):
    # this function should not print, just visit
    # the int is for depth, used when printing spaces
    stack = [(head, 0)]
    nodes = []

    while stack:
        cur = stack.pop()
        node = cur[0]
        depth = cur[1]
        print('{} > {} {} ms'.format(
            depth*'    ',
            node.get('type'),
            int(node.get('time_in_nanos'))/1000
        ))
        children = node.get('children')
        if children:
            for c in children:
                stack.append((c, depth+1))

    return nodes


def display(by_shard):
    # Only print data from one shard now
    shard = by_shard[0]
    searches = shard.get('searches')

    # aggregations = shard.get('aggregations')

    if searches:
        for s in searches:
            # don't care about other queries now
            # just trying this out
            for q in s['query']:
                visit_tree(q)


def main():
    try:
        parsed = parse_stdin(sys.stdin.read())
    except ParseException as err:
        print(err.message)

    display(parsed)


if __name__ == "__main__":
    main()
