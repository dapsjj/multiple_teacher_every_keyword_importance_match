#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import MeCab
import re
import collections
import pymssql
import datetime
import time
import logging
import os
import configparser
import decimal #must add this line


conn = None  # 连接
cur = None  # 游标

IGNORE_WORDS = set([])  # 重要度計算外とする語
no_need_words = ["これ","ここ","こと","それ","ため","よう","さん","そこ","たち","ところ","それぞれ","これら","どれ","br","ます","です","する","\"\""]
no_need_words1 = ["？","?"]
no_need_words2 = ["っ","ぁ","ぃ","ぅ","ぇ","ヶ"]
keep_words1 = ["ccc","CCC"]

# ひらがな
JP_HIRA = set([chr(i) for i in range(12353, 12436)])
# カタカナ
JP_KATA = set([chr(i) for i in range(12449, 12532+1)])
#要忽略的字符
MULTIBYTE_MARK = set([
    '、', ',', '，', '。', '．','\'', '”', '“', '《', '》', '：', '（', '）', '(', ')', '；', '.', '・', '～', '`',
    '%', '％', '$', '￥', '~', '■', '●', '◆', '×', '※', '►', '▲', '▼', '‣', '·', '∶', ':', '‐', '_', '‼', '≫',
    '－','−', ';', '･', '〈', '〉', '「', '」', '『', '』', '【', '】', '〔', '〕', '?', '？', '!', '！', '+', '-',
    '*', '÷', '±', '…', '‘', '’', '／', '/', '<', '>', '><', '[', ']', '#', '＃', '゛', '゜',
    # '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    # '０','１', '２', '３', '４', '５', '６', '７', '８', '９',
    '①', '②', '③', '④', '⑤', '⑥', '⑦', '⑧', '⑨',
    '⑩', '⑪', '⑫', '⑬', '⑭', '⑮', '⑯', '⑰', '⑱', '⑲', '⑳',
    '➀', '➁', '➂', '➃', '➄', '➅', '➆', '➇', '➈', '➉',
    '⑴', '⑵', '⑶', '⑷', '⑸', '⑹', '⑺', '⑻', '⑼', '⑽',
    '⑾', '⑿', '⒀', '⒁', '⒂', '⒃', '⒄', '⒅', '⒆', '⒇',
    '⒈', '⒉', '⒊', '⒋', '⒌', '⒍', '⒎', '⒏', '⒐', '⒑',
    '⒒', '⒓', '⒔', '⒕', '⒖', '⒗', '⒘', '⒙', '⒚', '⒛',
    'ⅰ', 'ⅱ', 'ⅲ', 'ⅳ', 'ⅴ', 'ⅵ', 'ⅶ', 'ⅷ', 'ⅸ', 'ⅹ',
    'Ⅰ', 'Ⅱ', 'Ⅲ', 'Ⅳ', 'Ⅴ', 'Ⅵ', 'Ⅶ', 'Ⅷ', 'Ⅸ', 'Ⅹ',
    'Ⅺ', 'Ⅻ', '❶', '❷', '❸', '❹', '❺', '❻', '❼', '❽', '❾', '❿',
    '⓫', '⓬', '⓭', '⓮', '⓯', '⓰', '⓱', '⓲', '⓳', '⓴',
    '㈠', '㈡', '㈢', '㈣', '㈤', '㈥', '㈦', '㈧', '㈨', '㈩',
    '㊀', '㊁', '㊂', '㊃', '㊄', '㊅', '㊆', '㊇', '㊈', '㊉',
    'Ⓐ', 'Ⓑ', 'Ⓒ', 'Ⓓ', 'Ⓔ', 'Ⓕ', 'Ⓖ', 'Ⓗ', 'Ⓘ', 'Ⓙ',
    'Ⓚ', 'Ⓛ', 'Ⓜ', 'Ⓝ', 'Ⓞ', 'Ⓟ', 'Ⓠ', 'Ⓡ', 'Ⓢ', 'Ⓣ',
    'Ⓤ', 'Ⓥ', 'Ⓦ', 'Ⓧ', 'Ⓨ', 'Ⓩ', 'ⓐ', 'ⓑ', 'ⓒ', 'ⓓ',
    'ⓔ', 'ⓕ', 'ⓖ', 'ⓗ', 'ⓘ', 'ⓙ', 'ⓚ', 'ⓛ', 'ⓜ', 'ⓝ',
    'ⓞ', 'ⓟ', 'ⓠ', 'ⓡ', 'ⓢ', 'ⓣ', 'ⓤ', 'ⓥ', 'ⓦ', 'ⓧ',
    'ⓨ', 'ⓩ', '⒜', '⒝', '⒞', '⒟', '⒠', '⒡', '⒢', '⒣',
    '⒤', '⒥', '⒦', '⒧', '⒨', '⒩', '⒪', '⒫', '⒬', '⒭',
    '⒮', '⒯', '⒰', '⒱', '⒲', '⒳', '⒴', '⒵',
    '\r\n', '\t', '\n', '\\',
    '◇', '＜', '＞', '＊', '＝', '◍', '＋', '○', '―', 'ˇ', 'ˉ',
    '¨', '〃', '—', '‖', '∧', '∨', '∑', '∏', '∪', '∩', '∈',
    '∷', '√', '⊥', '∥', '∠', '⌒', '⊙', '∫', '∮', '≡', '≌',
    '≈', '∽', '∝', '≠', '≮', '≯', '≤', '≥', '∞', '∵', '∴',
    '♂', '♀', '°', '′', '″', '℃', '＄', '¤', '￠', '￡', '‰',
    '§', '№', '☆', '★', '□', '〓', '〜', '⬜', '〇', '＿',
    '▢', '∟', '⇒', '◯', '△', '✕', '＆', '|', '＠', '@', '&',
    '〖', '〗', '◎', '〒', '℉', '﹪', '﹫', '㎡', '㏕', '㎜',
    '㎝', '㎞', '㏎', 'm', '㎎', '㎏', '㏄', 'º', '¹', '²', '³',
    '↑', '↓', '←', '→', '↖', '↗', '↘', '↙', '↔', '↕', '➻', '➼',
    '➽', '➸', '➳', '➺', '➴', '➵', '➶', '➷', '➹', '▶', '▷',
    '◁', '◀', '◄', '«', '»', '➩', '➪', '➫', '➬', '➭', '➮',
    '➯', '➱', '⏎', '➲', '➾', '➔', '➘', '➙', '➚', '➛', '➜',
    '➝', '➞', '➟', '➠', '➡', '➢', '➣', '➤', '➥', '➦', '➧',
    '➨', '↚', '↛', '↜', '↝', '↞', '↟', '↠', '↡', '↢', '↣', '↤', '↥',
    '↦', '↧', '↨', '⇄', '⇅', '⇆', '⇇', '⇈', '⇉', '⇊', '⇋', '⇌', '⇍',
    '⇎', '⇏', '⇐', '⇑', '⇓', '⇔', '⇖', '⇗', '⇘', '⇙', '⇜', '↩', '↪',
    '↫', '↬', '↭', '↮', '↯', '↰', '↱', '↲', '↳', '↴', '↵', '↶', '↷',
    '↸', '↹', '☇', '☈', '↼', '↽', '↾', '↿', '⇀', '⇁', '⇂', '⇃', '⇞',
    '⇟', '⇠', '⇡', '⇢', '⇣', '⇤', '⇥', '⇦', '⇧', '⇨', '⇩', '⇪', '↺',
    '↻', '⇚', '⇛', '♐', '┌', '┍', '┎', '┏', '┐', '┑', '┒', '┓', '└', '┕',
    '┖', '┗', '┘', '┙', '┚', '┛', '├', '┝', '┞', '┟', '┠', '┡', '┢', '┣',
    '┤', '┥', '┦', '┧', '┨', '┩', '┪', '┫', '┬', '┭', '┮', '┯', '┰', '┱',
    '┲', '┳', '┴', '┵', '┶', '┷', '┸', '┹', '┺', '┻', '┼', '┽', '┾', '┿',
    '╀', '╁', '╂', '╃', '╄', '╅', '╆', '╇', '╈', '╉', '╊', '╋', '╌', '╍',
    '╎', '╏', '═', '║', '╒', '╓', '╔', '╕', '╖', '╗', '╘', '╙', '╚', '╛',
    '╜', '╝', '╞', '╟', '╠', '╡', '╢', '╣', '╤', '╥', '╦', '╧', '╨', '╩',
    '╪', '╫', '╬', '◤', '◥', '◣', '◢', '▸', '◂', '▴', '▾', '▽', '⊿', '▻',
    '◅', '▵', '▿', '▹', '◃', '❏', '❐', '❑', '❒', '▀', '▁', '▂', '▃', '▄',
    '▅', '▆', '▇', '▉', '▊', '▋', '█', '▌', '▍', '▎', '▏', '▐', '░', '▒', '▓',
    '▔', '▕', '▣', '▤', '▥', '▦', '▧', '▨', '▩', '▪', '▫', '▬', '▭', '▮', '▯',
    '㋀', '㋁', '㋂', '㋃', '㋄', '㋅', '㋆', '㋇', '㋈', '㋉', '㋊', '㋋',
    '㏠', '㏡', '㏢', '㏣', '㏤', '㏥', '㏦', '㏧', '㏨', '㏩', '㏪', '㏫',
    '㏬', '㏭', '㏮', '㏯', '㏰', '㏱', '㏲', '㏳', '㏴', '㏵', '㏶', '㏷',
    '㏸', '㏹', '㏺', '㏻', '㏼', '㏽', '㏾', '㍙', '㍚', '㍛', '㍜', '㍝',
    '㍞', '㍟', '㍠', '㍡', '㍢', '㍣', '㍤', '㍥', '㍦', '㍧', '㍨', '㍩',
    '㍪', '㍫', '㍬', '㍭', '㍮', '㍯', '㍰', '㍘', '☰', '☲', '☱', '☴',
    '☵', '☶', '☳', '☷', '☯', '♠', '♣', '♧', '♡', '♥', '❤', '❥', '❣',
    '✲', '☀', '☼', '☾', '☽', '◐', '◑', '☺', '☻', '☎', '☏', '✿', '❀',
    '¿', '½', '✡', '㍿', '卍', '卐', '✚', '♪', '♫', '♩', '♬', '㊚', '㊛',
    '囍', '㊒', '㊖', 'Φ', 'Ψ', '♭', '♯', '♮', '¶', '€', '¥', '﹢', '﹣',
    '=', '≦', '≧', '≒', '﹤', '﹥', '㏒', '㏑', '⅟', '⅓', '⅕', '⅙',
    '⅛', '⅔', '⅖', '⅚', '⅜', '¾', '⅗', '⅝', '⅞', '⅘', '≂', '≃', '≄',
    '≅', '≆', '≇', '≉', '≊', '≋', '≍', '≎', '≏', '≐', '≑', '≓', '≔',
    '≕', '≖', '≗', '≘', '≙', '≚', '≛', '≜', '≝', '≞', '≟', '≢', '≣',
    '≨', '≩', '⊰', '⊱', '⋛', '⋚', '∬', '∭', '∯', '∰', '∱', '∲', '∳',
    '℅', '‱', 'ø', 'Ø', 'π', 'ღ', '♤', '＇', '〝', '〞', 'ˆ', '﹕', '︰',
    '﹔', '﹖', '﹑', '•', '¸', '´', '｜', '＂', '｀', '¡', '﹏', '﹋',
    '﹌', '︴', '﹟', '﹩', '﹠', '﹡', '﹦', '￣', '¯', '﹨', '˜', '﹍', '﹎',
    '﹉', '﹊', '‹', '›', '﹛', '﹜', '［', '］', '{', '}', '︵', '︷', '︿',
    '︹', '︽', '﹁', '﹃', '︻', '︶', '︸', '﹀', '︺', '︾', '﹂', '﹄',
    '︼', '❝', '❞', '£', 'Ұ', '₴', '₰', '¢', '₤', '₳', '₲', '₪', '₵',
    '₣', '₱', '฿', '₡', '₮', '₭', '₩', 'ރ', '₢', '₥', '₫', '₦',
    'z', 'ł', '﷼', '₠', '₧', '₯', '₨', 'K', 'č', 'र', '₹', 'ƒ', '₸',
    '✐', '✎', '✏', '✑', '✒', '✍', '✉', '✁', '✂', '✃', '✄', '✆',
    '☑', '✓', '✔', '☐', '☒', '✗', '✘', 'ㄨ', '✖', '☢', '☠', '☣', '✈',
    '☜', '☞', '☝', '☚', '☛', '☟', '✌', '♢', '♦', '☁', '☂', '❄', '☃',
    '♨', '웃', '유', '❖', '☪', '✪', '✯', '☭', '✙', '⚘', '♔', '♕', '♖',
    '♗', '♘', '♙', '♚', '♛', '♜', '♝', '♞', '♟', '◊', '◦', '◘', '◈', 'の',
    'Ю', '❈', '✣', '✤', '✥', '✦', '❉', '❦', '❧', '❃', '❂', '❁', '☄', '☊',
    '☋', '☌', '☍', '۰', '⊕', 'Θ', '㊣', '◙', '♈', '큐', '™', '◕', '‿', '｡'
    # "a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z",
    # "A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"
    ])


def cmp_noun_list(data):
    """
    和文テキストを受け取り、複合語（空白区切りの単名詞）のリストを返す
    """
    savetxt_list=[]
    mecab = MeCab.Tagger("-Ochasen") #有词性标注的
    # mecab = MeCab.Tagger("-Owakati")  # 没有词性标注的
    cmp_nouns = mecab.parse(data)
    every_row = cmp_nouns.split('\n')
    save_word_list = []
    for every_attribute_line in every_row:
        every_attribute_array = every_attribute_line.split('\t')
        if len(every_attribute_array) > 3:
            save_word_list.append([every_attribute_array[0].strip(), every_attribute_array[3].strip()])
    length_save_word_list = len(save_word_list)
    for i in range(length_save_word_list):
        if i == 0:
            if save_word_list[i][1].find('名詞') != -1:
                if save_word_list[i + 1][1].find('名詞') != -1:
                    savetxt_list.append(save_word_list[i][0] + save_word_list[i + 1][0])
                elif save_word_list[i + 1][1].find('名詞') != -1 and save_word_list[i + 2][1].find('名詞') != -1:
                    savetxt_list.append(save_word_list[i][0] + save_word_list[i + 1][0]) + save_word_list[i + 2][0]
                elif save_word_list[i + 1][1].find('名詞') != -1 and save_word_list[i + 2][1].find('名詞') != -1 and \
                                save_word_list[i + 3][1].find('名詞') != -1:
                    savetxt_list.append(save_word_list[i][0] + save_word_list[i + 1][0] + save_word_list[i + 2][0] +
                                        save_word_list[i + 3][0])
                else:
                    savetxt_list.append(save_word_list[i][0])
            else:
                savetxt_list.append(save_word_list[i][0])
        elif i > 0 and i < (length_save_word_list - 4):
            if save_word_list[i][1].find('名詞') != -1 and save_word_list[i - 1][1].find('名詞') == -1:
                if save_word_list[i + 1][1].find('名詞') != -1 and save_word_list[i + 2][1].find('名詞') != -1 and \
                                save_word_list[i + 3][1].find('名詞') != -1:
                    savetxt_list.append(save_word_list[i][0] + save_word_list[i + 1][0] + save_word_list[i + 2][0] +
                                        save_word_list[i + 3][0])
                elif save_word_list[i + 1][1].find('名詞') != -1 and save_word_list[i + 2][1].find('名詞') != -1:
                    savetxt_list.append(save_word_list[i][0] + save_word_list[i + 1][0] + save_word_list[i + 2][0])
                elif save_word_list[i + 1][1].find('名詞') != -1:
                    savetxt_list.append(save_word_list[i][0] + save_word_list[i + 1][0])
                else:
                    savetxt_list.append(save_word_list[i][0])
            elif save_word_list[i][1].find('名詞') == -1:
                savetxt_list.append(save_word_list[i][0])
        elif i > 0 and i == (length_save_word_list - 4):
            if save_word_list[i][1].find('名詞') != -1 and save_word_list[i - 1][1].find('名詞') == -1:
                if save_word_list[i + 1][1].find('名詞') != -1 and save_word_list[i + 2][1].find('名詞') != -1 and \
                                save_word_list[i + 3][1].find('名詞') != -1:
                    savetxt_list.append(save_word_list[i][0] + save_word_list[i + 1][0] + save_word_list[i + 2][0] +
                                        save_word_list[i + 3][0])
                elif save_word_list[i + 1][1].find('名詞') != -1 and save_word_list[i + 2][1].find('名詞') != -1:
                    savetxt_list.append(save_word_list[i][0] + save_word_list[i + 1][0] + save_word_list[i + 2][0])
                elif save_word_list[i + 1][1].find('名詞') != -1:
                    savetxt_list.append(save_word_list[i][0] + save_word_list[i + 1][0])
                else:
                    savetxt_list.append(save_word_list[i][0])
            elif save_word_list[i][1].find('名詞') == -1:
                savetxt_list.append(save_word_list[i][0])
        elif i > 0 and i == (length_save_word_list - 3):
            if save_word_list[i][1].find('名詞') != -1 and save_word_list[i - 1][1].find('名詞') == -1:
                if save_word_list[i + 1][1].find('名詞') != -1 and save_word_list[i + 2][1].find('名詞') != -1:
                    savetxt_list.append(save_word_list[i][0] + save_word_list[i + 1][0] + save_word_list[i + 2][0])
                elif save_word_list[i + 1][1].find('名詞') != -1:
                    savetxt_list.append(save_word_list[i][0] + save_word_list[i + 1][0])
                else:
                    savetxt_list.append(save_word_list[i][0])
            elif save_word_list[i][1].find('名詞') == -1:
                savetxt_list.append(save_word_list[i][0])
        elif i > 0 and i == (length_save_word_list - 2):
            if save_word_list[i][1].find('名詞') != -1 and save_word_list[i - 1][1].find('名詞') == -1:
                if save_word_list[i + 1][1].find('名詞') != -1:
                    savetxt_list.append(save_word_list[i][0] + save_word_list[i + 1][0])
                else:
                    savetxt_list.append(save_word_list[i][0])
            elif save_word_list[i][1].find('名詞') == -1:
                savetxt_list.append(save_word_list[i][0])
        elif i > 0 and i == (length_save_word_list - 1):
            if save_word_list[i][1].find('名詞') != -1 and save_word_list[i - 1][1].find('名詞') == -1:
                savetxt_list.append(save_word_list[i][0])
            elif save_word_list[i][1].find('名詞') == -1:
                savetxt_list.append(save_word_list[i][0])

    # savetxt_list = [' '.join(i) for i in savetxt_list]  # 不加这一句,重要度就是频率

    new_txt_list = []
    for every_word in savetxt_list:  # 每个字符都不在特殊符号里并且不是数字的词语添加到new_txt_list
        append_flag = True
        if (every_word is not None and len(every_word.strip()) > 1 and not (every_word.strip().isdigit())):
            for i in every_word:
                if i in MULTIBYTE_MARK:
                    append_flag = False
                    break
            if append_flag == True:
                new_txt_list.append(every_word)

    new_txt_list2 = []
    for every_word in new_txt_list:  # 不包含no_need_words的词加入到new_txt_list2
        find_flag = False
        for word in no_need_words:
            if every_word.find(word) != -1:
                find_flag = True
                break
        if find_flag == False:
            new_txt_list2.append(every_word)

    new_txt_list3 = []
    for every_word in new_txt_list2:  # 去掉0和片假名长音'ー'开头的字符串
        if not every_word.startswith('0') and not every_word.startswith('０') and not every_word.startswith('ー'):
            new_txt_list3.append(every_word)

    new_txt_list4 = no_need_keyword_remove(new_txt_list3)
    new_txt_list4 = [' '.join(i) for i in new_txt_list4]  # 不加这一句,重要度就是频率
    cmp_nouns = new_txt_list4
    return cmp_nouns


def _increase(cmp_nouns, terms):
    """
    専門用語リストへ、整形して追加するサブルーチン
    """
    if len(terms) > 1:
        cmp_noun = ' '.join(terms)
        cmp_nouns.append(cmp_noun)
    del terms[:]


def cmp_noun_dict(data):
    """
    複合語（単名詞の空白区切り）をキーに、その出現回数を値にしたディクショナリを返す
    """
    cmp_noun = cmp_noun_list(data)
    return list2dict(cmp_noun)


def list2dict(list_data):
    """
    リストの要素をキーに、その出現回数を値にしたディクショナリを返す
    """
    dict_data = {}
    for data in list_data:
        if data in dict_data:
            dict_data[data] += 1
        else:
            dict_data[data] = 1
    return dict_data


def score_lr(frequency, ignore_words=None, average_rate=1, lr_mode=1, dbm=None):
    """
    専門用語とそれを構成する単名詞の情報から重要度を計算する
        cmp_noun
            複合語（単名詞の空白区切り）をキーに出現回数を値に
            したディクショナリ
        ignore_word
            重要度計算の例外とする語のリスト
        average_rate
            重要度計算においてLRとFrequencyの比重を調整する
            数値が小さいほうがLRの比重が大きい
        lr_mode
            1のときはLRの計算において「延べ数」をとる
            2のときはLRの計算において「異なり数」をとる
    """
    # 対応する関数を呼び出し
    if dbm is None:
        noun_importance = _score_lr_dict(frequency, ignore_words, average_rate, lr_mode)
    else:
        noun_importance = _score_lr_dbm(frequency, ignore_words, average_rate, lr_mode, dbm)
    return noun_importance


def _score_lr_dbm(frequency, ignore_words=None, average_rate=1, lr_mode=1, dbm=None):
    """
    dbmに蓄積したLR情報をもとにLRのスコアを出す
    """
    # 「専門用語」をキーに、値を「重要度」にしたディクショナリ
    noun_importance = {}
    stat = dbm    # 単名詞ごとの連接情報
    for cmp_noun in frequency.keys():
        importance = 1       # 専門用語全体の重要度
        count = 0     # 専門用語中の単名詞数をカウント
        if re.match(r"\s*$", cmp_noun):
            continue
        for noun in cmp_noun.split(" "):
            if re.match(r"[\d\.\,]+$", noun):
                continue
            left_score = 0
            right_score = 0
            if noun in stat:
                value = stat[noun].decode("utf-8").split("\t")
                if lr_mode == 1:  # 連接語の”延べ数”をとる場合
                    left_score = int(value[0])
                    right_score = int(value[1])
                elif lr_mode == 2:  # 連接語の”異なり数”をとる場合
                    left_score = int(value[3])
                    right_score = int(value[4])
            if noun not in ignore_words and not re.match(r"[\d\.\,]+$", noun):
                importance *= (left_score + 1) * (right_score + 1)
                count += 1
        if count == 0:
            count = 1
        # 相乗平均でLR重要度を出す
        importance = importance ** (1 / (2 * average_rate * count))
        noun_importance[cmp_noun] = importance
        count = 0
    return noun_importance


def _score_lr_dict(frequency, ignore_words, average_rate=1, lr_mode=1):
    # 「専門用語」をキーに、値を「重要度」にしたディクショナリ
    noun_importance = {}
    stat = {}  # 単名詞ごとの連接情報
    # 専門用語ごとにループ
    for cmp_noun in frequency.keys():
        if not cmp_noun:
            continue
        org_nouns = cmp_noun.split(" ")
        nouns = []
        # 数値及び指定の語を重要度計算から除外
        for noun in org_nouns:
            if ignore_words:
                if noun in ignore_words:
                    continue
            elif re.match(r"[\d\.\,]+$", noun):
                continue
            nouns.append(noun)
        # 複合語の場合、連接語の情報をディクショナリに入れる
        if len(nouns) > 1:
            for i in range(0, len(nouns)-1):
                if not nouns[i] in stat:
                    stat[nouns[i]] = [0, 0]
                if not nouns[i+1] in stat:
                    stat[nouns[i+1]] = [0, 0]
                if lr_mode == 1:  # 連接語の”延べ数”をとる場合
                    stat[nouns[i]][0] += frequency[cmp_noun]
                    stat[nouns[i+1]][1] += frequency[cmp_noun]
                elif lr_mode == 2:   # 連接語の”異なり数”をとる場合
                    stat[nouns[i]][0] += 1
                    stat[nouns[i+1]][1] += 1
    for cmp_noun in frequency.keys():
        importance = 1  # 専門用語全体の重要度
        count = 0  # ループカウンター（専門用語中の単名詞数をカウント）
        if re.match(r"\s*$", cmp_noun):
            continue
        for noun in cmp_noun.split(" "):
            if re.match(r"[\d\.\,]+$", noun):
                continue
            left_score = 0
            right_score = 0
            if noun in stat:
                left_score = stat[noun][0]
                right_score = stat[noun][1]
            importance *= (left_score + 1) * (right_score + 1)
            count += 1
        if count == 0:
            count = 1
        # 相乗平均でlr重要度を出す
        importance = importance ** (1 / (2 * average_rate * count))
        noun_importance[cmp_noun] = importance
        count = 0
    return noun_importance


def term_importance(*args):
    """
    複数のディクショナリの値同士を乗算する
    """
    master = {}
    new_master = {}
    for noun_dict in args:
        for nouns, importance in noun_dict.items():
            if nouns in master:
                # new_master[nouns] = master[nouns] * importance
                new_master[nouns] = [master[nouns][0], master[nouns][0] * importance]
            else:
                new_master[nouns] = [importance]
        master = new_master.copy()
    return master


def modify_agglutinative_lang(data):
    """
    半角スペースで区切られた単名詞を膠着言語（日本語等）向けに成形する
    """
    data_disp = ""
    eng = 0
    eng_pre = 0
    for noun in data.split(" "):
        if re.match("[A-Z|a-z]+$", noun):
            eng = 1
        else:
            eng = 0
        # 前後ともアルファベットなら半角空白空け、それ以外なら区切りなしで連結
        if eng and eng_pre:
            data_disp = data_disp + " " + noun
        else:
            data_disp = data_disp + noun
        eng_pre = eng
    return data_disp


def calculate_importance_for_total(president_txt,member_txt):
    tatext_president = president_txt
    tatext_member = member_txt
    # 複合語を抽出し、重要度を算出
    frequency_president = cmp_noun_dict(tatext_president)
    frequency_member = cmp_noun_dict(tatext_member)
    LR_president = score_lr(frequency_president, ignore_words=IGNORE_WORDS, lr_mode=1, average_rate=1)
    term_imp_president = term_importance(frequency_president, LR_president)
    LR_member = score_lr(frequency_member, ignore_words=IGNORE_WORDS, lr_mode=1, average_rate=1)
    term_imp_member = term_importance(frequency_member, LR_member)
    # 重要度が高い順に並べ替えて出力
    data_collection_president = collections.Counter(term_imp_president)
    data_collection_member = collections.Counter(term_imp_member)
    totalImportance_president, totalImportance_member = 0, 0
    key_words_lenth_president = len(data_collection_president)
    key_words_lenth_member = len(data_collection_member)
    key_words_list_president = []
    for cmp_noun, value in data_collection_president.most_common():
        totalImportance_president += value[1]
        key_words_list_president.append(cmp_noun)
    key_words_list_memeber = []
    sum_matching_degree = 0
    matches_keywords_count = 0  # 社员参照教师报告后,与教师有多少个关键字匹配
    for cmp_noun, value in data_collection_member.most_common():
        para_keyword = modify_agglutinative_lang(cmp_noun)
        totalImportance_member += value[1]
        key_words_list_memeber.append(cmp_noun)
        if cmp_noun in key_words_list_president:
            matches_keywords_count += 1
            sum_matching_degree += value[1]
    return key_words_lenth_president,key_words_lenth_member,totalImportance_president,totalImportance_member,sum_matching_degree,matches_keywords_count


def calculate_importance_for_detail(president_txt,member_txt):
    tatext_president = president_txt
    tatext_member = member_txt
    # 複合語を抽出し、重要度を算出
    frequency_president = cmp_noun_dict(tatext_president)
    frequency_member = cmp_noun_dict(tatext_member)
    LR_president = score_lr(frequency_president, ignore_words=IGNORE_WORDS, lr_mode=1, average_rate=1)
    term_imp_president = term_importance(frequency_president, LR_president)
    LR_member = score_lr(frequency_member, ignore_words=IGNORE_WORDS, lr_mode=1, average_rate=1)
    term_imp_member = term_importance(frequency_member, LR_member)
    # 重要度が高い順に並べ替えて出力
    data_collection_president = collections.Counter(term_imp_president)
    data_collection_member = collections.Counter(term_imp_member)
    totalImportance_president, totalImportance_member = 0, 0 #教师重要度与社员重要度合计
    key_words_lenth_president = len(data_collection_president)
    key_words_lenth_member = len(data_collection_member)
    key_words_list_president = [] #教师词列表
    key_words_list_memeber = []  #社员词列表
    member_sum_matching_degree = 0  # 社员参照教师TOP后,与教师有关键字匹配上的词的重要度合计(社员的)
    member_matches_keywords_count = 0  # 社员参照教师TOP后,与教师有多少个关键字匹配(社员的)
    frequency_ratio = 0 #社员所有能与教师的词匹配上的词的频度合计/教师所有的词的频度合计
    importance_ratio = 0 #社员所有能与教师的词匹配上的词的重要度合计/教师所有的词的重要度合计
    append_list = []

    for cmp_noun, value in data_collection_member.most_common(): #社员
        totalImportance_member += value[1]  #社员重要度合计
        key_words_list_memeber.append(cmp_noun)  #社员的词列表

    for cmp_noun, value in data_collection_president.most_common(): #教师
        totalImportance_president += value[1] #教师重要度合计
        key_words_list_president.append(cmp_noun) #教师词列表

        para_keyword = modify_agglutinative_lang(cmp_noun)
        para_president_importance_degree = value[1]  # 教师每个词的重要度
        para_president_keyword_frequency = value[0]  # 教师每个词的频度
        para_member_matched_keyword_frequency = 0  # 社员与教师当前的词能匹配到的词频初始化为0
        para_member_matched_keyword_importance_degree = 0  # 社员与教师当前的词能匹配到的词的重要度初始化为0
        if cmp_noun in key_words_list_memeber:  # 如果教师的词出现在社员的词中
            para_member_matched_keyword_importance_degree = data_collection_member.get(cmp_noun, 0)[1]   # 能匹配上社员的词,则社员的这个词的重要度就是社员自己TOP中的这个词的重要度
            para_member_matched_keyword_frequency = data_collection_member.get(cmp_noun, 0)[0]  # 能匹配上社员的词,则社员的这个词的频度就是社员自己TOP中的这个词的频度
            member_matches_keywords_count += 1
            member_sum_matching_degree += data_collection_member.get(cmp_noun, 0)[1]  # 社员マッチ度合计
        frequency_ratio = float(para_member_matched_keyword_frequency/para_president_keyword_frequency)
        importance_ratio = float(para_member_matched_keyword_importance_degree/para_president_importance_degree)
        append_list.append([para_keyword,para_president_importance_degree,para_member_matched_keyword_importance_degree,para_president_keyword_frequency,para_member_matched_keyword_frequency,frequency_ratio,importance_ratio])
    return append_list


    '''
    for cmp_noun, value in data_collection_member.most_common(): #社员
        para_keyword = modify_agglutinative_lang(cmp_noun)
        para_report_year = report_year #年
        para_report_week = coef_week_list[0] #周
        para_member_importance_degree = value[1] #社员每个词的重要度
        para_member_match_degree = 0 #社员每个词的マッチ度初始化为0
        para_president_matched_keyword_frequency = 0 #教师与社员当前的词能匹配到的词频初始化为0
        para_president_matched_keyword_importance_degree = 0 #教师与社员当前的词能匹配到的词的重要度初始化为0
        if cmp_noun in key_words_list_president: #如果社员的词出现在教师的词中
            para_member_match_degree = value[1] #能匹配上教师的词,则这个词マッチ度就是社员自己TOP中的这个词的重要度
            para_president_matched_keyword_frequency = data_collection_president.get(cmp_noun,0)[0] #如果社员的这个词在教师的词列表中出现过,则取教师这个词的频度
            para_president_matched_keyword_importance_degree = data_collection_president.get(cmp_noun,0)[1] #如果社员的这个词在教师的词列表中出现过,则取教师这个词的重要度
            member_matches_keywords_count += 1
            member_sum_matching_degree += value[1] #社员マッチ度合计
        para_member_keyword_frequency = value[0] #社员每个词的频度
    '''



def get_year_week_from_Mst_date(current_date):
    '''
    :param current_date:系统当前日期年-月-日
    :return:Mst_date表返回的当前年和当前周
    '''
    try:
        sql = " select year_no,week_no from Mst_date where date_mst='%s' "  % current_date
        cur.execute(sql)
        rows = cur.fetchall()
        if rows:
            current_year = rows[0][0]
            current_week = rows[0][1]
            return current_year,current_week
        else:
            return ""
    except pymssql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method get_year_week_from_Mst_date() error!Can not query from table Mst_date!")
        logger.error("Exception:" + str(ex))
        raise ex


def read_dateConfig_file_set_database():
    if os.path.exists(os.path.join(os.path.dirname(__file__), "dateConfig.ini")):
        try:
            conf = configparser.ConfigParser()
            conf.read(os.path.join(os.path.dirname(__file__), "dateConfig.ini"), encoding="utf-8-sig")
            server = conf.get("server", "server")
            user = conf.get("user", "user")
            password = conf.get("password", "password")
            database = conf.get("database", "database")
            return server,user,password,database
        except Exception as ex:
            logger.error("Content in dateConfig.ini about database has error.")
            logger.error("Exception:" + str(ex))
            raise ex



def read_dateConfig_file_set_year_week():
    global report_year
    global coef_week_list
    if os.path.exists(os.path.join(os.path.dirname(__file__), "dateConfig.ini")):
        try:
            conf = configparser.ConfigParser()
            conf.read(os.path.join(os.path.dirname(__file__), "dateConfig.ini"), encoding="utf-8-sig")
            year = conf.get("execute_year", "year")
            week = conf.get("execute_week", "week")
            if  year:
                report_year = year
            if week:
                coef_week_list = [int(week)]
        except Exception as ex:
            logger.error("Content in dateConfig.ini about execute_year or execute_week has error.")
            logger.error("Exception:" + str(ex))
            raise ex


def read_report_from_database(report_week,employee_code,report_year):
    '''
    :param report_year:top报告年份
    :param report_week:top报告周
    :param employee_code:社员号
    :return:top报告内容
    '''
    try:
        sql = "select remark from report where report_year =%s and report_week =%s and employee_code =%s" \
              % (report_year, report_week, employee_code)
        cur.execute(sql)
        rows = cur.fetchall()
        if rows!=[]:
            content = rows[0][0].replace("<br>", "")
            return content
        else:
            return ""
    except pymssql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method read_report_from_database() error!Can not query from table report!")
        logger.error("Exception:" + str(ex))
        raise ex



def generate_employeelist_total_data(employee_teacher_list, report_week_list, report_year):
    '''
    :param employee_teacher_list:要插入到rreport_est_automatic_multiple_teacher表的社员号+教师号列表
    :param report_year:top报告年份
    :param report_week_list:top报告周列表
    :return:要插入到report_est_automatic_multiple_teacher表的list
    '''
    if employee_teacher_list and report_week_list and report_year:
        report_year = report_year#年
        report_week_list = report_week_list#周列表
        employee_teacher_list = employee_teacher_list#社员号+教师号列表
        report_est_automatic_list = []
        for week in report_week_list:
            for employee_teacher in employee_teacher_list:
                teacher_report = read_report_from_database(week, employee_teacher[1], report_year)  # 教师TOP报告内容
                employee_report = read_report_from_database(week, employee_teacher[0], report_year)  # 社员TOP报告内容
                if teacher_report and employee_report:
                    result = calculate_importance_for_total(teacher_report, employee_report)
                    importance_degree = str(result[3])
                    matching_degree = str(result[4])
                    add_list = [str(report_year),str(week),str(employee_teacher[0]),employee_teacher[1],importance_degree,matching_degree]
                    report_est_automatic_list.append(add_list)
        return report_est_automatic_list
    else:
        logger.error("Call method generate_employeelist_total_data() error!There is a null value in the parameters.")
        raise


def generate_employeelist_detail_data(employee_teacher_list, report_week_list, report_year):
    '''
    :param employee_teacher_list:要插入到rreport_est_automatic_multiple_teacher表的社员号+教师号列表
    :param report_year:top报告年份
    :param report_week_list:top报告周列表
    :return:要插入到report_est_automatic_multiple_teacher表的list
    '''
    if employee_teacher_list and report_week_list and report_year:
        report_year = report_year#年
        report_week_list = report_week_list#周列表
        employee_teacher_list = employee_teacher_list#社员号+教师号列表
        report_est_automatic_list = []
        for week in report_week_list:
            for employee_teacher in employee_teacher_list:
                teacher_report = read_report_from_database(week, employee_teacher[1], report_year)  # 教师TOP报告内容
                employee_report = read_report_from_database(week, employee_teacher[0], report_year)  # 社员TOP报告内容
                if teacher_report and employee_report:
                    result = calculate_importance_for_detail(teacher_report, employee_report)
                    add_list = [[str(report_year),str(week),str(employee_teacher[0]),employee_teacher[1],*item] for item in result]
                    # report_est_automatic_list.append(add_list)
                    for item in add_list:
                        report_est_automatic_list.append(item)
        return report_est_automatic_list
    else:
        logger.error("Call method generate_employeelist_detail_data() error!There is a null value in the parameters.")
        raise


def get_employee_teacher_list():
    '''
       :return:report_target表去重后的社员号+教师号列表
       '''
    try:
        sql = " select distinct cast(employee_code as int),cast(teacher_code as int) from report_target_multiple_teacher_for_noun "
        cur.execute(sql)
        rows = cur.fetchall()
        employee_teacher_list = []
        if rows:
            for row in rows:
                employee_teacher_list.append(list(row))
            return employee_teacher_list
        else:
            return ""
    except pymssql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method get_employee_teacher_list() error!")
        logger.error("Exception:" + str(ex))
        raise ex


def insert_report_est_multiple_teacher_noun_total(datalist):
    '''
    :param datalist:要插入数据的列表
    :return:无
    '''
    if datalist:
        report_est_multiple_teacher_noun_total_list = []
        try:
            for one_row in datalist:
                report_year = one_row[0]
                report_week = one_row[1]
                employee_code = one_row[2]
                check_code = one_row[3]
                importance_degree = one_row[4]
                matching_degree = one_row[5]
                report_est_multiple_teacher_noun_total_list.append([report_year,report_week,employee_code,check_code,importance_degree,matching_degree])
            report_est_multiple_teacher_noun_total_list = [tuple(item) for item in report_est_multiple_teacher_noun_total_list]
            sql = ' insert into report_est_multiple_teacher_noun_total (report_year, report_week, employee_code,check_code,importance_degree,matching_degree) ' \
                  ' values(%s, %s, %s, %s, %s, %s) '
            cur.executemany(sql, report_est_multiple_teacher_noun_total_list)
            conn.commit()
        except pymssql.Error as ex:
            logger.error("dbException:" + str(ex))
            raise ex
        except Exception as ex:
            logger.error("Call method insert_report_est_multiple_teacher_noun_total() error!")
            logger.error("Exception:"+str(ex))
            conn.rollback()
            raise ex


def insert_report_est_multiple_teacher_for_noun_detail(datalist):
    '''
    :param datalist:要插入数据的列表
    :return:无
    '''
    if datalist:
        report_est_multiple_teacher_for_noun_detail = []
        try:
            '''
            for one_row in datalist:
                report_year = one_row[0]
                report_week = one_row[1]
                employee_code = one_row[2]
                check_code = one_row[3]
                keyword = one_row[4]
                teacher_current_keyword_importance_degree = one_row[5]
                member_current_keyword_importance_degree = one_row[6]
                teacher_current_keyword_match_frequency = one_row[7]
                member_current_keyword_match_frequency = one_row[8]
                frequency_ratio = one_row[9]
                importance_ratio = one_row[10]
                report_est_multiple_teacher_for_noun_detail.append([report_year,report_week,employee_code,check_code,keyword,teacher_current_keyword_importance_degree,member_current_keyword_importance_degree,teacher_current_keyword_match_frequency,teacher_current_keyword_match_frequency,member_current_keyword_match_frequency,frequency_ratio,importance_ratio])
            '''
            report_est_multiple_teacher_noun_total_list = [tuple(item) for item in datalist]
            sql = ' insert into report_est_multiple_teacher_for_noun_detail (report_year, report_week, employee_code,check_code,keyword,teacher_current_keyword_importance_degree,member_current_keyword_importance_degree,teacher_current_keyword_match_frequency,member_current_keyword_match_frequency,frequency_ratio,importance_ratio) ' \
                  ' values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) '
            cur.executemany(sql, report_est_multiple_teacher_noun_total_list)
            conn.commit()
        except pymssql.Error as ex:
            logger.error("dbException:" + str(ex))
            raise ex
        except Exception as ex:
            logger.error("Call method insert_report_est_multiple_teacher_noun_total() error!")
            logger.error("Exception:"+str(ex))
            conn.rollback()
            raise ex


def delete_current_data_from_report_est_multiple_teacher_noun_total(report_year,report_week):
    '''
    :param report_year:top报告年份
    :param report_week:top报告周
    :return:无
    '''
    try:
        sql = ' delete from report_est_multiple_teacher_noun_total where report_year = %s and report_week = %s' \
              % (report_year, report_week[0])
        cur.execute(sql)
        conn.commit()
    except pymssql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method delete_current_data_from_report_est_multiple_teacher_noun_total() error!")
        logger.error("Exception:" + str(ex))
        conn.rollback()
        raise ex


def delete_current_data_from_report_est_multiple_teacher_for_noun_detail(report_year,report_week):
    '''
    :param report_year:top报告年份
    :param report_week:top报告周
    :return:无
    '''
    try:
        sql = ' delete from report_est_multiple_teacher_for_noun_detail where report_year = %s and report_week = %s' \
              % (report_year, report_week[0])
        cur.execute(sql)
        conn.commit()
    except pymssql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method delete_current_data_from_report_est_multiple_teacher_for_noun_detail() error!")
        logger.error("Exception:" + str(ex))
        conn.rollback()
        raise ex


def write_log():
    '''
    :return: 返回logger对象
    '''
    # 获取logger实例，如果参数为空则返回root logger
    logger = logging.getLogger()
    now_date = datetime.datetime.now().strftime('%Y%m%d')
    log_file = now_date+".log"# 文件日志
    if not os.path.exists("log"):#python文件同级别创建log文件夹
        os.makedirs("log")
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)s line:%(lineno)s %(message)s')
    file_handler = logging.FileHandler("log" + os.sep + log_file, mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter) # 可以通过setFormatter指定输出格式
    # 为logger添加的日志处理器，可以自定义日志处理器让其输出到其他地方
    logger.addHandler(file_handler)
    # 指定日志的最低输出级别，默认为WARN级别
    logger.setLevel(logging.INFO)
    return logger


def getConn():
    '''
    声明数据库连接对象
    '''
    global conn
    global cur
    try:
        conn = pymssql.connect(server, user, password, database)
        cur = conn.cursor()
    except pymssql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method getConn() error!")
        raise ex


def closeConn():
    '''
    关闭数据库连接对象
    '''
    global conn
    global cur
    try:
        cur.close()
        conn.close()
    except pymssql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method closeConn() error!")
        raise ex


def no_need_keyword_remove(para_keyword_list):
    '''
    处理要保留的关键字和不保留的关键字
    :return:返回处理后的List
    '''
    keepWordList1=[]
    keepWordList2=[]
    keepWordList3=[]
    removeList=[]
    if para_keyword_list:
        try:
            for item1 in para_keyword_list:
                flag1 = False
                for mark1 in no_need_words1: #["？","?"]
                    if item1.find(mark1)!=-1:
                        flag1 = True
                        removeList.append(item1)
                        break
                if flag1==False:
                    keepWordList1.append(item1)

            for item2 in keepWordList1:
                flag2 = False
                for mark2 in no_need_words2: #["っ","ぁ","ぃ","ぅ","ぇ","ヶ"]
                    if item2.startswith(mark2) or item2.endswith(mark2):
                        flag2 = True
                        removeList.append(item2)
                        break
                if flag2 == False:
                    keepWordList2.append(item2)

            for item3 in keepWordList2:
                if item3 in keep_words1: # ["ccc","CCC"]
                    keepWordList3.append(item3)
                else:
                    if len(item3)<3: #长度小于3的词加入到List
                        keepWordList3.append(item3)
                    elif len(item3)>=3: #长度大于3的词
                        str_repeat_list = [everyChar for everyChar in item3] #把词中的每一个字放到str_repeat_list
                        str_repeat_list = list(set(str_repeat_list)) #利用set特性去重
                        if len(str_repeat_list)>1: #不全是同一个字符则加入到List中
                            keepWordList3.append(item3)
                        else:
                            removeList.append(item3)
            return keepWordList3
        except Exception as ex:
            logger.error("Call method no_need_keyword_remove() error!")
            logger.error("Exception:" + str(ex))
            raise ex



if __name__=="__main__":
    logger = write_log()  # 获取日志对象
    time_start = datetime.datetime.now()
    start = time.perf_counter()
    logger.info("Program start,now time is:"+str(time_start))
    server,user,password,database = read_dateConfig_file_set_database()#读取配置文件中的数据库信息
    getConn()  # 数据库连接对象
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")#系统当前日期
    current_year,current_week = get_year_week_from_Mst_date(current_date)#从Mst_date获取当前年和周
    report_year = str(current_year)#要处理的年
    coef_week_list = [current_week] #要处理的周
    read_dateConfig_file_set_year_week()#读配置文件设置report_year和coef_week_list
    logger.info("report_year:" + report_year)
    logger.info("report_week:" + str(coef_week_list[0]))
    employee_and_teacher_list = get_employee_teacher_list()#从report_target_multiple_teacher_for_noun_detail表获取员工号和教师号列表(返回社员号和教师号列表)
    delete_current_data_from_report_est_multiple_teacher_noun_total(report_year, coef_week_list)#从report_est_multiple_teacher_noun_total表删除当前周数据
    data_addto_report_est_multiple_teacher_noun_total = generate_employeelist_total_data(employee_and_teacher_list, coef_week_list,report_year)#生成X年、X周、社员号X、教师数据的社员号X、重要度X、匹配度X
    insert_report_est_multiple_teacher_noun_total(data_addto_report_est_multiple_teacher_noun_total)#插入到report_est_multiple_teacher_noun_total
    delete_current_data_from_report_est_multiple_teacher_for_noun_detail(report_year,coef_week_list)# 从report_est_multiple_teacher_for_noun_detail表删除当前周数据
    data_addto_report_est_multiple_teacher_for_noun_detail = generate_employeelist_detail_data(employee_and_teacher_list, coef_week_list, report_year)  # 生成X年、X周、社员号X、教师数据的社员号X、重要度X、匹配度X
    insert_report_est_multiple_teacher_for_noun_detail(data_addto_report_est_multiple_teacher_for_noun_detail)  # 插入到report_est_multiple_teacher_for_noun_detail
    closeConn()
    time_end = datetime.datetime.now()
    end = time.perf_counter()
    logger.info("Program end,now time is:"+str(time_end))
    logger.info("Program run : %f seconds" % (end - start))



