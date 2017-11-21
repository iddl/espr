#!/usr/bin/env python

import argparse
import json
import sys

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


def tree_to_list(head):
    # simple stack based dfs to create a
    # printable list, nothing fancy
    stack = [(head, 0)]
    nodes = []

    while stack:
        cur = stack.pop()
        node = cur[0]
        depth = cur[1]

        # extract attributes along with
        # depth attribute
        processed_node = {
            'depth': depth,
        }
        processed_node.update(node)
        nodes.append(processed_node)

        children = node.get('children')
        if children:
            for c in children:
                stack.append((c, depth+1))

    return nodes


def print_node(node, verbose=False):
    INDENT = '   '
    depth = node.get('depth', 0)

    print('{}> {} {} ms'.format(
        depth*INDENT,
        node.get('type'),
        int(node.get('time_in_nanos'))/1000
    ))

    # optional breakdown
    breakdown = node.get('breakdown')
    if verbose and breakdown:
        for key, value in breakdown.items():
            print('{} {}: {}'.format(
                (depth)*INDENT,
                key,
                value
            ))


def display(by_shard, verbose=False):
    for s in by_shard:
        print('Shard: {0}'.format(s.get('id')))

        searches = s.get('searches')
        if searches:
            for s in searches:
                # don't care about other queries now
                # just trying this out
                for q in s['query']:
                    ordered_nodes = tree_to_list(q)
                    for n in ordered_nodes:
                        print_node(n, verbose=verbose)

        aggregations = s.get('aggregations')
        if aggregations:
            for a in aggregations:
                ordered_nodes = tree_to_list(a)
                for n in ordered_nodes:
                    print_node(n, verbose=verbose)

        print('')


def main():
    argparser = argparse.ArgumentParser(description='Process ES profile output.')
    argparser.add_argument('--verbose', '-v', action='count')
    args = argparser.parse_args()

    try:
        parsed = parse_stdin(sys.stdin.read())
    except ParseException as err:
        print(err.message)

    display(parsed, args.verbose)


if __name__ == "__main__":
    main()
