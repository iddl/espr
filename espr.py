#!/usr/bin/env python

import argparse
import json
import sys


VERBOSE_ABOVE_THRESHOLD = 1
VERBOSE_ALL = 2

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class ParseException(Exception):
    def __init__(self, message):
        self.message = message

    def __repr__(self):
        print(self.message)


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


def print_node(node, millis_threshold, verbose=0):
    INDENT = '   '
    depth = node.get('depth', 0)

    name = node.get('type', node.get('name'))
    millis = nanos_to_millis(node.get('time_in_nanos'))
    content = f'{depth * INDENT}> {name} {millis} ms'
    above_threshold = millis >= millis_threshold
    prefix = f'{bcolors.FAIL}{bcolors.BOLD}' if above_threshold else ''
    suffix = bcolors.ENDC if prefix else ''
    print(f'{prefix}{content}{suffix}')

    # verbose output
    if verbose == VERBOSE_ALL or verbose == VERBOSE_ABOVE_THRESHOLD and above_threshold:
        description = node.get('description')
        if description:
            print(f'{(depth + 1) * INDENT}description: {description}')

        breakdown = node.get('breakdown')
        if breakdown:
            for key, value in breakdown.items():
                breakdown_millis = nanos_to_millis(value)
                breakdown_above_threshold = breakdown_millis >= millis_threshold
                breakdown_prefix = f'{bcolors.FAIL}{bcolors.BOLD}' if breakdown_above_threshold else ''
                breakdown_suffix = f'{bcolors.ENDC}' if breakdown_prefix else ''
                breakdown_content = '{}{}: {}'.format((depth + 1) * INDENT, key, breakdown_millis)
                print(f'{breakdown_prefix}{breakdown_content}{breakdown_suffix}')


def nanos_to_millis(time_in_nanos):
    return int(time_in_nanos) / 1000000


def millis_to_nanos(time_in_millis):
    return time_in_millis * 1000000


def mutably_prune_fast_operations(data, millis_threshold):
    # mutate "data" by removing operations that fall below the millis threshold
    nanos_threshold = millis_to_nanos(millis_threshold)
    for shard in data['profile']['shards']:
        if 'searches' in shard:
            for search in shard['searches']:
                search['query'] = [mutably_recursivly_prune_fast_children(query, nanos_threshold) for query in search['query'] if query['time_in_nanos'] > nanos_threshold]
                if not search['query']:
                    del search['query']
                if search['rewrite_time'] < nanos_threshold:
                    del search['rewrite_time']
                search['collector'] = [mutably_recursivly_prune_fast_children(collector, nanos_threshold) for collector in search['collector'] if collector['time_in_nanos'] > nanos_threshold]
                if not search['collector']:
                    del search['collector']
            shard['searches'] = [search for search in shard['searches'] if search]
            if not shard['searches']:
                del shard['searches']
        if 'aggregations' in shard:
            shard['aggregations'] = [mutably_recursivly_prune_fast_children(aggregation, nanos_threshold) for aggregation in shard['aggregations'] if aggregation['time_in_nanos'] > nanos_threshold]
            if not shard['aggregations']:
                del shard['aggregations']
    data['profile']['shards'] = [shard for shard in data['profile']['shards'] if len(shard) > 1]


def mutably_recursivly_prune_fast_children(element, nanos_threshold):
    if 'children' in element:
        element['children'] = [mutably_recursivly_prune_fast_children(child, nanos_threshold) for child in element['children'] if child['time_in_nanos'] > nanos_threshold]

    return element


def display(data, millis_threshold=None, max_depth=None, verbose=0):
    hits_count = data.get('hits', {}).get('total')
    shards_count = data.get("_shards", {}).get("total")
    took_millis = data.get("took")
    print(f'Took {took_millis}ms to query {shards_count} shards for {hits_count} hits')


    try:
        by_shard = data['profile']['shards']
    except KeyError:
        raise ParseException('data does not contain profile info')

    print(f'Profile data for {len(by_shard)} shards shown')
    print()

    for shard in by_shard:
        print('Shard: {0}'.format(shard.get('id')))

        searches = shard.get('searches')
        if searches:
            for search in searches:
                for q in search.get('query', []):
                    ordered_nodes = tree_to_list(q)
                    for n in ordered_nodes:
                        if not max_depth or n['depth'] < max_depth:
                            print_node(n, millis_threshold, verbose=verbose)
                if 'rewrite_time' in search:
                    print(f'> rewrite_time {nanos_to_millis(search["rewrite_time"])} ms')
                for c in search.get('collector', []):
                    for collector in tree_to_list(c):
                        if not max_depth or collector['depth'] < max_depth:
                            print_node(collector, millis_threshold, verbose=verbose)

        aggregations = shard.get('aggregations')
        if aggregations:
            for a in aggregations:
                ordered_nodes = tree_to_list(a)
                for n in ordered_nodes:
                    if not max_depth or n['depth'] < max_depth:
                        print_node(n, millis_threshold, verbose=verbose)

        print('')


def main():
    argparser = argparse.ArgumentParser(description='Process ES profile output.')
    argparser.add_argument('--verbose', '-v', action='count',
                           help='Specify once to show high verbosity output for operations over the --millis threshold. '
                           'Specify twice to show high verbosity for everything.')
    argparser.add_argument('file', nargs='?',
                           help='Specify a file to read as input. If not given uses stdin.')
    argparser.add_argument('--millis', type=int, default=1000,
                           help='Threshold in milliseconds you want to highlight and dig into')
    argparser.add_argument('--exclude-below-millis', type=int, default=0,
                           help='Threshold in milliseconds you want to exclude')
    argparser.add_argument('--depth', type=int, default=0,
                           help='Maximum depth of children to display')
    args = argparser.parse_args()

    try:
        if args.file:
            with open(args.file) as f:
                parsed = json.loads(f.read())
        else:
            parsed = json.loads(sys.stdin.read())
    except ParseException as err:
        print(err.message)
        sys.exit(1)

    if args.exclude_below_millis != 0:
        mutably_prune_fast_operations(parsed, args.exclude_below_millis)
    display(parsed, args.millis, args.depth, args.verbose)


if __name__ == "__main__":
    main()
