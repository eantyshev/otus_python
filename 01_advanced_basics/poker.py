#!/usr/bin/env python
# -*- coding: utf-8 -*-

# -----------------
# Реализуйте функцию best_hand, которая принимает на вход
# покерную "руку" (hand) из 7ми карт и возвращает лучшую
# (относительно значения, возвращаемого hand_rank)
# "руку" из 5ти карт. У каждой карты есть масть(suit) и
# ранг(rank)
# Масти: трефы(clubs, C), пики(spades, S), червы(hearts, H), бубны(diamonds, D)
# Ранги: 2, 3, 4, 5, 6, 7, 8, 9, 10 (ten, T), валет (jack, J), дама (queen, Q), король (king, K), туз (ace, A)
# Например: AS - туз пик (ace of spades), TH - дестяка черв (ten of hearts), 3C - тройка треф (three of clubs)

# Задание со *
# Реализуйте функцию best_wild_hand, которая принимает на вход
# покерную "руку" (hand) из 7ми карт и возвращает лучшую
# (относительно значения, возвращаемого hand_rank)
# "руку" из 5ти карт. Кроме прочего в данном варианте "рука"
# может включать джокера. Джокеры могут заменить карту любой
# масти и ранга того же цвета, в колоде два джокерва.
# Черный джокер '?B' может быть использован в качестве треф
# или пик любого ранга, красный джокер '?R' - в качестве черв и бубен
# любого ранга.

# Одна функция уже реализована, сигнатуры и описания других даны.
# Вам наверняка пригодится itertoolsю
# Можно свободно определять свои функции и т.п.
# -----------------

import itertools

def hand_rank(hand):
    """Возвращает значение определяющее ранг 'руки'"""
    ranks = card_ranks(hand)
    if straight(ranks) and flush(hand):
        return (8, max(ranks))
    elif kind(4, ranks):
        return (7, kind(4, ranks), kind(1, ranks))
    elif kind(3, ranks) and kind(2, ranks):
        return (6, kind(3, ranks), kind(2, ranks))
    elif flush(hand):
        return (5, ranks)
    elif straight(ranks):
        return (4, max(ranks))
    elif kind(3, ranks):
        return (3, kind(3, ranks), ranks)
    elif two_pair(ranks):
        return (2, two_pair(ranks), ranks)
    elif kind(2, ranks):
        return (1, kind(2, ranks), ranks)
    else:
        return (0, ranks)


def _card_rank(card):
    _chr = card[0]
    chr2value = {"T": 10, "J": 11, "Q": 12, "K": 13, "A": 14}
    if _chr in chr2value:
        return chr2value[_chr]
    else:
        return int(_chr)


def card_ranks(hand):
    """Возвращает список рангов (его числовой эквивалент),
    отсортированный от большего к меньшему"""
    return sorted([_card_rank(c) for c in hand], reverse=True)


def flush(hand):
    """Возвращает True, если все карты одной масти"""
    suit = hand[0][1]
    return all([card[1] == suit for card in hand[1:]])


def straight(ranks):
    """Возвращает True, если отсортированные ранги формируют последовательность 5ти,
    где у 5ти карт ранги идут по порядку (стрит)"""
    return ranks == [ranks[0] + k for k in range(0, -5, -1)]


def kind(n, ranks):
    """Возвращает первый ранг, который n раз встречается в данной руке.
    Возвращает None, если ничего не найдено"""
    for k, g in itertools.groupby(ranks):
        if len(list(g)) == n:
            return k


def two_pair(ranks):
    """Если есть две пары, то возврщает два соответствующих ранга,
    иначе возвращает None"""
    pair = []
    for k, g in itertools.groupby(ranks):
        if len(list(g)) >= 2:
            pair.append(k)
    if len(pair) == 2:
        return pair


def best_hand(hand):
    """Из "руки" в 7 карт возвращает лучшую "руку" в 5 карт """
    return max(itertools.combinations(hand, 5),
               key=lambda h: hand_rank(h))

RANKS = [str(i) for i in range(5, 10)] + ['T','J','Q','K','A']


def _expand_joker(hand, joker, joker_suits):
    substitutions = [
        i[0] + i[1]
        for i in itertools.product(RANKS, joker_suits)
    ]
    if joker in hand:
        for subst in substitutions:
            if subst not in hand:
                yield [subst if c == joker else c for c in hand]
    else:
        yield hand

def _expand_many(hands, it):
    return itertools.chain(*[it(h) for h in hands])

def _wild_combinations(hand):
    hands = itertools.combinations(hand, 5)
    return  _expand_many(
                _expand_many(hands,
                             lambda h: _expand_joker(h, '?R', ['H', 'D'])
                            ),
                lambda h: _expand_joker(h, '?B', ['C', 'S'])
            )

    
def best_wild_hand(hand):
    """best_hand но с джокерами"""
    return max(_wild_combinations(hand),
               key=lambda h: hand_rank(h))

def test_best_hand():
    print "test_best_hand..."
    assert (sorted(best_hand("6C 7C 8C 9C TC 5C JS".split()))
            == ['6C', '7C', '8C', '9C', 'TC'])
    assert (sorted(best_hand("TD TC TH 7C 7D 8C 8S".split()))
            == ['8C', '8S', 'TC', 'TD', 'TH'])
    assert (sorted(best_hand("JD TC TH 7C 7D 7S 7H".split()))
            == ['7C', '7D', '7H', '7S', 'JD'])
    print 'OK'


def test_best_wild_hand():
    print "test_best_wild_hand..."
    assert (sorted(best_wild_hand("6C 7C 8C 9C TC 5C ?B".split()))
            == ['7C', '8C', '9C', 'JC', 'TC'])
    assert (sorted(best_wild_hand("TD TC 5H 5C 7C ?R ?B".split()))
            == ['7C', 'TC', 'TD', 'TH', 'TS'])
    assert (sorted(best_wild_hand("JD TC TH 7C 7D 7S 7H".split()))
            == ['7C', '7D', '7H', '7S', 'JD'])
    print 'OK'

if __name__ == '__main__':
    test_best_hand()
    test_best_wild_hand()
