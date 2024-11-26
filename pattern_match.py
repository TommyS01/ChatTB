import os
import nltk
import re

from nltk import word_tokenize, pos_tag

from nltk.corpus import stopwords
from numpy.ma.core import equal

# from nltk.misc.sort import selection

nltk.download('punkt_tab')
nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger_eng')


stopWords = set(stopwords.words('english'))
excludeWords = ["all", "between"]
customStopWords = set([word for word in stopWords if word not in excludeWords])
extraWords = ["broken", "return", "show", "select", "find", "total", ",", ".", "!", "give", "between", "greater,"
                "less", "bigger", "cheaper", "faster", "taller", "smaller", "more", "amount", "group", "grouped"]

greaterList = ['more', "greater", "faster", "taller", "higher", ">"]
lesserList = ["less", "smaller", "cheaper", "lower", "slower", "shorter", "<", "fewer", "lesser"]
equalList = ['=', 'equal', 'matching', 'equals']

maxList = ['max', 'maximum', 'highest', 'tallest', 'greatest', 'most', 'fastest', 'maximal']
minList = ['min', 'minimum', 'least', 'smallest', 'cheapest', 'lowest', 'slowest', 'shortest', 'fewest', 'minimal']
sumList = ['sum', 'summation']
avgList = ['avg', 'average', 'ave', 'mean']
countList = ['cnt', 'count']
aggList = maxList + minList + sumList + avgList + countList

ascList = ['asc', 'ascending', 'increasing', 'growing', 'scaling']
descList = ['desc', 'descending', 'decreasing', 'lowering']
orderingList = descList + ascList

db = "mongo"

def translate_query(sentence, db, table=""):
    def targeter (targets, maxList, minList, avgList, sumList, countList):
        for i in targets:
            if i in maxList:
                targInd = targets.index(i) + 1
                aggVal = "max(" + targets[targInd] + ")"
                # print(aggVal)
                filteredClauses[0].remove(i)
                filteredClauses[0].remove(targets[targInd])
                filteredClauses[0].append(aggVal)
            elif i in minList:
                # print('asdfasdfasdfasf')
                targInd = targets.index(i) + 1
                aggVal = "min(" + targets[targInd] + ")"
                # print(aggVal)
                filteredClauses[0].remove(i)
                filteredClauses[0].remove(targets[targInd])
                filteredClauses[0].append(aggVal)
            elif i in avgList:
                targInd = targets.index(i) + 1
                aggVal = "avg(" + targets[targInd] + ")"
                # print(aggVal)
                filteredClauses[0].remove(i)
                filteredClauses[0].remove(targets[targInd])
                filteredClauses[0].append(aggVal)
            elif i in sumList:
                targInd = targets.index(i) + 1
                aggVal = "sum(" + targets[targInd] + ")"
                # print(aggVal)
                filteredClauses[0].remove(i)
                filteredClauses[0].remove(targets[targInd])
                filteredClauses[0].append(aggVal)
            elif i in countList:
                targInd = targets.index(i) + 1
                aggVal = "count(" + targets[targInd] + ")"
                # print(aggVal)
                filteredClauses[0].remove(i)
                filteredClauses[0].remove(targets[targInd])
                filteredClauses[0].append(aggVal)

    def selectionFilter (selections, filteredClauses):
        # print(filteredClauses)
        if filteredClauses[0][0] in ["all", "everything", "*"]:
            selections = "*"
            return selections
        else:
            for selection in filteredClauses[0]:
                selections += selection + ", "
            return selections[:-2]  # get rid of that final comma

    def splitList(input_list, split_values):
        result = []
        sublist = []

        for item in input_list:
            if item in split_values:
                result.append(sublist)
                sublist = []
            else:
                sublist.append(item)

        if sublist:
            result.append(sublist)

        return result

    def selector(filteredClauses):
        selections = ""
        if any(item in aggList for item in targets):
            targeter(targets, maxList, minList, avgList, sumList, countList)

            return selectionFilter(selections, filteredClauses)
        else:
            return selectionFilter(selections, filteredClauses)

    def comparisonConverter(input_word):
        if input_word in greaterList:
            return ">"
        elif input_word in lesserList:
            return "<"
        elif input_word in equalList:
            return "="

    def orderConverter(input):
        if input in descList:
            return " DESC"
        elif input in ascList:
            return " ASC"

    def joinWorder(clause):
        if "right" in clause:
            joinWord = " RIGHT JOIN "
        elif "left" in clause:
            joinWord = " LEFT JOIN "
        elif "inner" in clause:
            joinWord = " INNER JOIN "
        elif "outer" in clause:
            joinWord = " FULL OUTER JOIN "
        else:
            joinWord = " JOIN "

        return joinWord

    def mongoJoinSplitter(statement):
        clauses = {
            "SELECT": [],
            "FROM": [],
            "JOIN": [],
            "WHERE": [],
            "ORDER BY": []
        }

        # Define regular expressions for each clause
        clause_patterns = {
            "SELECT": r"SELECT (.+?) FROM",
            "FROM": r"FROM (.+?)(?: RIGHT| LEFT| INNER| FULL| OUTER| JOIN|$)",
            "JOIN": r"JOIN (.+?)(?: WHERE| ORDER BY|$)",
            "WHERE": r"WHERE (.+?)(?: ORDER BY|$)",
            "ORDER BY": r"ORDER BY (.+?)$"
        }

        for clause_name, pattern in clause_patterns.items():
            match = re.search(pattern, statement, re.IGNORECASE)
            if match:
                values = [value.strip() for value in match.group(1).split(',')]
                clauses[clause_name] = values

        return clauses

    def mongoSplitter(statement):
        clauses = {
            "SELECT": [],
            "FROM": [],
            "WHERE": [],
            "GROUP BY": [],
            "HAVING": [],
            "ORDER BY": []
        }

        # Define regular expressions for each clause
        clause_patterns = {
            "SELECT": r"SELECT (.+?) FROM",
            "FROM": r"FROM (.+?)(?: WHERE| GROUP BY| HAVING| ORDER BY|$)",
            "WHERE": r"WHERE (.+?)(?: GROUP BY| HAVING| ORDER BY|$)",
            "GROUP BY": r"GROUP BY (.+?)(?: HAVING| ORDER BY|$)",
            "HAVING": r"HAVING (.+?)(?: ORDER BY|$)",
            "ORDER BY": r"ORDER BY (.+?)$"
        }

        for clause_name, pattern in clause_patterns.items():
            match = re.search(pattern, statement, re.IGNORECASE)
            if match:
                values = [value.strip() for value in match.group(1).split(',')]
                clauses[clause_name] = values

        return clauses

    def mongoSelector(selectionList):
        if selectionList[0] != "*":
            mongoSelections = ''
            for selection in selectionList:
                if selection[:4] == "max(" or selection[:4] == "min(" or selection[:4] == "sum(" or selection[:4] == "avg(" or selection[:6] == "count(":
                    selection = mongoAggConverter(selection)
                mongoSelections += '"' + selection + '": 1, '

        else:
            mongoSelections = ''
        return mongoSelections

    def mongoWherer(whereList):
        whereTokens = word_tokenize(whereList)
        if "AND" in whereTokens:
            # print(whereTokens)
            whereStatement = ('{"' + whereTokens[0] + '": {"' + mongoComparisonConverter(whereTokens[1]) + '": ' + whereTokens[2] + '}, "'
                                + whereTokens[4] + '": {"' + mongoComparisonConverter(whereTokens[5]) + '": ' + whereTokens[6] + '}}')
        else:
            whereStatement = '{"' + whereTokens[0] + '": {"' + mongoComparisonConverter(whereTokens[1]) + '": ' + whereTokens[2] + '}}'
        return whereStatement

    def mongoComparisonConverter(comparison):
        if comparison == ">":
            return "$gt"
        elif comparison == "<":
            return "$lt"
        else:
            return "$eq"

    def mongoAggConverter(selection):
        if selection[:4] == "max(":
            # print(selection+ 'asdfasdfasd')
            selectionAggList = selection[:-1].split("(")
            # print(selectionAggList)
            aggSelection = 'max' + selectionAggList[1] + '": {$max: "$' + selectionAggList[1] + '"}'
        elif selection[:4] == "min(":
            # print(selection)
            selectionAggList = selection[:-1].split("(")
            # print(selectionAggList)
            aggSelection = 'min' + selectionAggList[1] + '": {$min: "$' + selectionAggList[1] + '"}'
        elif selection[:4] == "sum(":
            # print(selection)
            selectionAggList = selection[:-1].split("(")
            # print(selectionAggList)
            aggSelection = 'sum' + selectionAggList[1] + '": {$sum: "$' + selectionAggList[1] + '"}'
        elif selection[:4] == "avg(":
            # print(selection)
            selectionAggList = selection[:-1].split("(")
            # print(selectionAggList)
            aggSelection = 'avg' + selectionAggList[1] + '": {$avg: "$' + selectionAggList[1] + '"}'
        elif selection[:6] == "count(":
            # print(selection)
            selectionAggList = selection[:-1].split("(")
            # print(selectionAggList)
            aggSelection = 'count' + selectionAggList[1] + '": {$sum: 1}'
        return aggSelection

    def mongoAgger(selectionList):
        for select in selectionList:
            if " AS agg" in select:
                selectAbbr = select.split(" AS agg")[0]
                print("selectAbbr", selectAbbr)
                select = selectAbbr
                print(select)
            # print("select", select)
            if select[:3] in aggList:
                # print(select + 'asdfagg')
                groupAgg = mongoAggConverter(select)
                print(groupAgg)
            # else:

        # print(groupAgg)
        return groupAgg

    def mongoAggProjector(selectionList):
        groupProj = []
        for select in selectionList:
            # print(select)
            if select[:3] not in aggList:
                groupProj.append(select)
                # print(groupProj)
        if groupProj != []:
            mongoProjections = mongoSelector(groupProj)
            return mongoProjections
        else:
            return ""

    #table = ""

    # Main loop
    selections = ""
    # Tokenize the sentence
    tokens = word_tokenize(sentence)

    order = ""
    for item in tokens:
        if item in orderingList:
            tokens.remove(item)
            order = item
    # print(order)
    if order != "":
        order = orderConverter(order)

    # Perform part-of-speech tagging
    tagged_tokens = pos_tag(tokens)

    # Extract key elements
    targets = []
    conditions = []
    actions = []
    comparisons = []
    numbers = []

    for word, tag in tagged_tokens:
        if tag == 'NN' or tag == 'NNS' or word in aggList:
            targets.append(word) # looking for nouns or specific adjectives as targets
        elif tag == 'VB' or tag == 'VBP' or tag == 'VBN':
            actions.append(word) # verbs are actions
        elif tag == 'IN' or tag == 'WRB' or tag == 'JJR' or tag == 'VBG':
            if word != "matching":
                conditions.append(word) # conditions are adverbs and prepositions
        if word in greaterList or word in lesserList or word in equalList:
            comparisons.append(word)
        if tag == 'CD':
            numbers.append(word)

    clauses = []
    filteredClauses = []

    clauseLists = splitList(tokens, conditions)
    for clause in clauseLists:
        clauses.append(clause)

    for clause in clauses:
        if not clause:
            clauses.remove(clause)
        else:
            filteredList = [word for word in clause if word.lower() not in customStopWords and word.lower() not in extraWords]
            filteredClauses.append(filteredList)

    # Print the extracted elements
    if actions:
        print("Action:", actions)
    if targets:
        print("Target:", targets)
    if conditions:
        print("Condition:", conditions)
    if comparisons:
        print("Comparisons:", comparisons)
    if numbers:
        print("Numbers:", numbers)

    orderIn = False
    for i in tokens:
        if i == "order":
            orderIn = True

    # print("tokens:", tokens)
    if "join" not in tokens:
        if orderIn == False:
            if len(filteredClauses) == 1:
                print("1")
                selections = selector(filteredClauses)
                if table == "":  # if we don't have a table, ask for it, we can assume to reuse it
                    #table = input("What table are you querying from? ")
                    pass
                statement = "SELECT " + selections + " FROM " + table
            elif len(filteredClauses) == 2:
                print("2")
                if len(conditions) == 1 and conditions[0] == "from":
                    selections = selector(filteredClauses)
                    table = targets[-1]
                    if table == "":
                        #table = input("What table are you querying from? ")
                        pass
                    elif conditions[0] == 'from':
                        table = filteredClauses[1][0]
                    statement = "SELECT " + selections + " FROM " + table
                elif len(conditions) == 1 and (
                        conditions[0] == 'where' or conditions[0] == 'when' or conditions[0] == 'wherever' or conditions[
                    0] == 'whenever' or conditions[0] == 'if'):
                    if comparisons:
                        selections = selector(filteredClauses)
                        if table == "":
                            #table = input("What table are you querying from? ")
                            pass
                        elif conditions[0] == 'from':
                            table = filteredClauses[1][0]
                        comparison = comparisonConverter(comparisons[0])
                        statement = ("SELECT " + selections + " FROM " + table + " WHERE " + filteredClauses[-1][0] + " "
                                    + comparison + " " + filteredClauses[-1][-1])
                    else:
                        statement = "Error"
                elif (conditions[0] == 'where' or conditions[0] == 'when' or conditions[0] == 'wherever' or conditions[
                    0] == 'whenever' or conditions[0] == 'if'):
                    selections = selector(filteredClauses)
                    if table == "":
                        #table = input("What table are you querying from? ")
                        pass
                    elif conditions[0] == 'from':
                        table = filteredClauses[1][0]
                else:
                    selections = selector(filteredClauses)
                    statement = "SELECT " + selections + " FROM " + table
            elif len(filteredClauses) == 3:
                print("3")
                selections = selector(filteredClauses)
                if table == "" and "from" not in conditions:
                    #table = input("What table are you querying from? ")
                    pass
                elif conditions[0] == 'from':
                    # print(filteredClauses)
                    table = filteredClauses[1][0]

                if "group" in targets:
                    if "where" not in comparisons:
                        statement = ("SELECT " + selections + " FROM " + table + " GROUP BY " + targets[-1])
                    else:
                        print("here")
                else:
                    if conditions[1] == "where":
                        # print(comparisons)
                        if len(comparisons) == 1:
                            comparison = comparisonConverter(comparisons[0])
                            statement = ("SELECT " + selections + " FROM " + table + " WHERE " + filteredClauses[-1][0] + " "
                                        + comparison + " " + numbers[0])
                        elif len(comparisons) == 2:
                            comparison1 = comparisonConverter(comparisons[0])
                            comparison2 = comparisonConverter(comparisons[-1])
                            statement = ("SELECT " + selections + " FROM " + table + " WHERE " + targets[2] + " "
                                        + comparison1 + " " + numbers[0] + " AND " + targets[3] + " "
                                        + comparison2 + " " + numbers[1])
            else:
                print("4")
                selections = selector(filteredClauses)
                if table == "" and "from" not in conditions:
                    #table = input("What table are you querying from? ")
                    pass
                elif conditions[0] == 'from':
                    print(filteredClauses)
                    table = filteredClauses[1][0]

                if "where" in conditions:
                    if "group" not in tokens:
                        comparison1 = comparisonConverter(comparisons[0])
                        comparison2 = comparisonConverter(comparisons[-1])
                        statement = ("SELECT " + selections + " FROM " + table + " WHERE " + filteredClauses[2][0] + " " +
                                    comparison1 + " " + numbers[0] + " AND " + filteredClauses[2][3] + " " +
                                    comparison2 + " " + numbers[1])
                    elif "having" not in tokens:
                        if len(comparisons) == 1:

                            if any(item in tokens for item in greaterList or lesserList or equalList):
                                print("here")

                            comparison = comparisonConverter(comparisons[0])
                            statement = ("SELECT " + selections + " FROM " + table + " WHERE " + filteredClauses[2][0] + " " +
                                        comparison + " " + numbers[0] + " GROUP BY " + targets[-1])
                        else:
                            comparison1 = comparisonConverter(comparisons[0])
                            comparison2 = comparisonConverter(comparisons[-1])
                            statement = ("SELECT " + selections + " FROM " + table + " WHERE " + filteredClauses[2][0] + " " +
                                        comparison1 + " " + filteredClauses[2][2] + " AND " + filteredClauses[2][3] + " " +
                                        comparison2 + " " + filteredClauses[2][-1] + " GROUP BY " + targets[-1])
                    else:
                        if len(comparisons) == 2: # one where and one having
                            comparison1 = comparisonConverter(comparisons[0])
                            comparison2 = comparisonConverter(comparisons[-1])

                            statement = ("SELECT " + selections + " FROM " + table + " WHERE " + filteredClauses[2][0] + " " +
                                        comparison1 + " " + numbers[0] + " GROUP BY " + filteredClauses[3][0] +
                                        " HAVING " + filteredClauses[-1][0] + " " + comparison2 + " " + numbers[1])
                        elif len(comparisons) == 3: # one and two
                            comparison1 = comparisonConverter(comparisons[0])
                            comparison2 = comparisonConverter(comparisons[1])
                            comparison3 = comparisonConverter(comparisons[2])

                            if (conditions[conditions.index("where") + 3]) in equalList + greaterList + lesserList:
                                print("in here")
                                statement = ("SELECT " + selections + " FROM " + table + " WHERE " + filteredClauses[2][0] +
                                            " " + comparison1 + " " + numbers[0] + " AND " + targets[-4] + " " + comparison2 +
                                            " " + numbers[1] + " GROUP BY " + filteredClauses[3][0] + " HAVING " +
                                            targets[-1] + " " + comparison3 + " " + numbers[2])
                            else:
                                statement = ("SELECT " + selections + " FROM " + table + " WHERE " + filteredClauses[2][0] +
                                            " " + comparison1 + " " + numbers[0] + " GROUP BY " + filteredClauses[3][0] +
                                            " HAVING " + filteredClauses[-1][0] + " " + comparison2 + " " + numbers[1] +
                                            " AND " + targets[-1] + " " + comparison3 + " " + numbers[2])
                        elif len(comparisons) == 4: # two and two
                            comparison1 = comparisonConverter(comparisons[0])
                            comparison2 = comparisonConverter(comparisons[1])
                            comparison3 = comparisonConverter(comparisons[2])
                            comparison4 = comparisonConverter(comparisons[-1])

                            statement = ("SELECT " + selections + " AS agg FROM " + table + " WHERE " + filteredClauses[3][0] + " " +
                                        comparison1 + " " + numbers[0] + " AND " + targets[5] + " " +
                                        comparison2 + " " + numbers[1] + " GROUP BY " + filteredClauses[4][0] +
                                        " HAVING " + targets[-2] + " " + comparison3 + " " + numbers[2] +
                                        " AND " + targets[-1] + " " + comparison4 + " " + numbers[3])
                else: # just having
                    if len(comparisons) == 1: # only one having condition
                        # print("ssdfsfdas")
                        comparison = comparisonConverter(comparisons[0])
                        statement = ("SELECT " + selections + " AS agg FROM " + table + " GROUP BY " + filteredClauses[2][0] + " HAVING " +
                                        filteredClauses[-1][0] + " " + comparison + " " + numbers[0] )
                    else: # multiple having conditions
                        comparison1 = comparisonConverter(comparisons[0])
                        comparison2 = comparisonConverter(comparisons[-1])
                        statement = ("SELECT " + selections + " AS agg FROM " + table + " GROUP BY " + filteredClauses[2][0] + " HAVING " +
                                    filteredClauses[-1][0] + " " + comparison1 + " " + numbers[0] + " AND " +
                                    targets[-1] + " " + comparison2 + " " + numbers[1])


        else: # order is in
            # print("here order")

            if len(filteredClauses) == 3:
                print("1")
                selections = selector(filteredClauses)
                table = filteredClauses[1][0]
                if table == "":  # if we don't have a table, ask for it, we can assume to reuse it
                    #table = input("What table are you querying from? ")
                    pass
                statement = "SELECT " + selections + " FROM " + table + " ORDER BY " + filteredClauses[2][0] + order
            elif len(filteredClauses) == 4:
                print("2")
                if len(comparisons) == 1 and conditions[0] == "from":
                    selections = selector(filteredClauses)
                    table = filteredClauses[1][0]
                    if table == "":
                        #table = input("What table are you querying from? ")
                        pass
                    elif conditions[0] == 'from':
                        table = filteredClauses[1][0]
                    comparison = comparisonConverter(comparisons[0])
                    statement = ("SELECT " + selections + " FROM " + table + " WHERE " + filteredClauses[2][0] + " " +
                                comparison + " " + numbers[0] + " ORDER BY " + filteredClauses[-1][0] + order)
                elif len(comparisons) == 2:
                    selections = selector(filteredClauses)
                    table = filteredClauses[1][0]
                    if table == "":
                        #table = input("What table are you querying from? ")
                        pass
                    elif conditions[0] == 'from':
                        table = filteredClauses[1][0]
                    comparison1 = comparisonConverter(comparisons[0])
                    comparison2 = comparisonConverter(comparisons[1])

                    statement = ("SELECT " + selections + " FROM " + table + " WHERE " + filteredClauses[2][0] + " " +
                                comparison1 + " " + numbers[0] + " AND " + targets[3] + " " + comparison2 + " " +
                                numbers[1] + " ORDER BY " + filteredClauses[-1][0] + order)
                elif "group" in tokens:
                    selections = selector(filteredClauses)
                    # print("hewrwerewrw")
                    statement = ("SELECT " + selections + " FROM " + table + " GROUP BY " + filteredClauses[2][0] +
                                " ORDER BY " + targets[-1] + order)
                else:
                    print("Error")
            elif len(filteredClauses) == 5:
                print("3")
                selections = selector(filteredClauses)
                if table == "" and "from" not in conditions:
                    #table = input("What table are you querying from? ")
                    pass
                elif conditions[0] == 'from':
                    print(filteredClauses)
                    table = filteredClauses[1][0]

                if "group" in targets:
                    if "where" not in tokens:
                        if len(comparisons) == 1:
                            # print("here")
                            comparison1 = comparisonConverter(comparisons[0])
                            statement = ("SELECT " + selections + " FROM " + table + " GROUP BY " + filteredClauses[3][0] +
                                        " HAVING " + filteredClauses[-2][0] + " " + comparison1 + " " + numbers[0] + " ORDER BY " +
                                        targets[-1] + order)
                        elif len(comparisons) == 2:
                            comparison1 = comparisonConverter(comparisons[0])
                            comparison2 = comparisonConverter(comparisons[1])
                            statement = ("SELECT " + selections + " FROM " + table + " GROUP BY " + filteredClauses[3][0] +
                                        " HAVING " + filteredClauses[-2][0] + " " + comparison1 + " " + numbers[0] + " AND " +
                                        targets[-3] + " " + comparison2 + " " + numbers[1] + " ORDER BY " + targets[-1] + order)
                        else:
                            statement = "Waiting on functionality"
                    elif "where" in tokens:
                        comparison1 = comparisonConverter(comparisons[0])
                        statement = ("SELECT " + selections + " FROM " + table + " WHERE " + filteredClauses[2][0] + " "  +
                                    comparison1 + " "  + numbers[0] + " GROUP BY " + filteredClauses[3][0] + " ORDER BY " +
                                    targets[-1] + order)
                else:
                    if conditions[1] == "where":
                        if len(comparisons) == 1:
                            comparison = comparisonConverter(comparisons[0])
                            statement = ("SELECT " + selections + " FROM " + table + " WHERE " + filteredClauses[-1][0] + " "
                                        + comparison + " " + numbers[0] + order)
                        elif len(comparisons) == 2:
                            comparison1 = comparisonConverter(comparisons[0])
                            comparison2 = comparisonConverter(comparisons[-1])
                            statement = ("SELECT " + selections + " FROM " + table + " WHERE " + targets[2] + " "
                                        + comparison1 + " " + numbers[0] + " AND " + targets[3] + " "
                                        + comparison2 + " " + numbers[1] + order)
            else:
                print("4")
                selections = selector(filteredClauses)
                if table == "" and "from" not in conditions:
                    #table = input("What table are you querying from? ")
                    pass
                elif conditions[0] == 'from':
                    print(filteredClauses)
                    table = filteredClauses[1][0]

                if "where" in conditions:
                    if "group" not in tokens:
                        comparison1 = comparisonConverter(comparisons[0])
                        comparison2 = comparisonConverter(comparisons[-1])
                        statement = ("SELECT " + selections + " FROM " + table + " WHERE " + filteredClauses[2][0] + " " +
                                    comparison1 + " " + numbers[0] + " AND " + filteredClauses[2][3] + " " +
                                    comparison2 + " " + numbers[1] + order)
                    elif "having" not in tokens:
                        if len(comparisons) == 1:

                            if any(item in tokens for item in greaterList or lesserList or equalList):
                                print("here")

                            comparison = comparisonConverter(comparisons[0])
                            statement = ("SELECT " + selections + " FROM " + table + " WHERE " + filteredClauses[2][0] + " " +
                                        comparison + " " + numbers[0] + " GROUP BY " + targets[-1] + order)
                        else:
                            comparison1 = comparisonConverter(comparisons[0])
                            comparison2 = comparisonConverter(comparisons[-1])
                            statement = ("SELECT " + selections + " FROM " + table + " WHERE " + filteredClauses[2][0] + " " +
                                        comparison1 + " " + filteredClauses[2][2] + " AND " + filteredClauses[2][3] + " " +
                                        comparison2 + " " + filteredClauses[2][-1] + " GROUP BY " + targets[-1] + order)
                    else:
                        if len(comparisons) == 2: # one where and one having
                            comparison1 = comparisonConverter(comparisons[0])
                            comparison2 = comparisonConverter(comparisons[-1])
                            # print("hasdfasdf")
                            statement = ("SELECT " + selections + " FROM " + table + " WHERE " + filteredClauses[2][0] + " " +
                                        comparison1 + " " + numbers[0] + " GROUP BY " + filteredClauses[3][0] +
                                        " HAVING " + filteredClauses[-2][0] + " " + comparison2 + " " + numbers[1] +
                                        " ORDER BY " + targets[-1] + order)
                        elif len(comparisons) == 3: # one and two
                            comparison1 = comparisonConverter(comparisons[0])
                            comparison2 = comparisonConverter(comparisons[1])
                            comparison3 = comparisonConverter(comparisons[2])

                            if (conditions[conditions.index("where") + 3]) in equalList + greaterList + lesserList:
                                # print("in here")
                                statement = ("SELECT " + selections + " FROM " + table + " WHERE " + filteredClauses[2][0] +
                                            " " + comparison1 + " " + numbers[0] + " AND " + targets[-4] + " " + comparison2 +
                                            " " + numbers[1] + " GROUP BY " + filteredClauses[3][0] + " HAVING " +
                                            targets[-3] + " " + comparison3 + " " + numbers[2] + " ORDER BY " + targets[-1] + order)
                            else:
                                statement = ("SELECT " + selections + " FROM " + table + " WHERE " + filteredClauses[2][0] +
                                            " " + comparison1 + " " + numbers[0] + " GROUP BY " + filteredClauses[3][0] +
                                            " HAVING " + filteredClauses[-1][0] + " " + comparison2 + " " + numbers[1] +
                                            " AND " + targets[-3] + " " + comparison3 + " " + numbers[2] + " ORDER BY " +
                                            targets[-1] + order)
                        elif len(comparisons) == 4: # two and two
                            comparison1 = comparisonConverter(comparisons[0])
                            comparison2 = comparisonConverter(comparisons[1])
                            comparison3 = comparisonConverter(comparisons[2])
                            comparison4 = comparisonConverter(comparisons[-1])

                            statement = ("SELECT " + selections + " FROM " + table + " WHERE " + filteredClauses[3][0] + " " +
                                        comparison1 + " " + numbers[0] + " AND " + targets[5] + " " +
                                        comparison2 + " " + numbers[1] + " GROUP BY " + filteredClauses[4][0] +
                                        " HAVING " + targets[-4] + " " + comparison3 + " " + numbers[2] +
                                        " AND " + targets[-3] + " " + comparison4 + " " + numbers[3] + " ORDER BY " + targets[-1]
                                        + order)
                else: # just having
                    if len(comparisons) == 1: # only one having condition
                        comparison = comparisonConverter(comparisons[0])
                        statement = ("SELECT " + selections + " FROM " + table + " GROUP BY " + filteredClauses[2][0] + " HAVING " +
                                        filteredClauses[-1][0] + " " + comparison + " " + numbers[0] + order)
                    else: # multiple having conditions
                        comparison1 = comparisonConverter(comparisons[0])
                        comparison2 = comparisonConverter(comparisons[-1])
                        statement = ("SELECT " + selections + " FROM " + table + " GROUP BY " + filteredClauses[2][0] + " HAVING " +
                                    filteredClauses[-1][0] + " " + comparison1 + " " + numbers[0] + " AND " +
                                    targets[-1] + " " + comparison2 + " " + numbers[1] + order)

        if db == "mongo":
            mongoClauses = mongoSplitter(statement)
            print(mongoClauses)

            mongoStatement = ''

            mongoTable = mongoClauses['FROM'][0]
            print(mongoTable)

            aggStatement = False
            for word in aggList:
                if word in statement:
                    aggStatement = True

            if len(mongoClauses['GROUP BY']) != 0:
                print("group")

                selectionList = mongoClauses['SELECT']
                mongoAggregation = mongoAgger(selectionList)
                mongoAggProj = mongoAggregation.split('"')[0]
                print("mongoAggProj", mongoAggProj)
                mongoProjection = mongoAggProjector(selectionList)
                # print(mongoProjection)
                mongoGroup = mongoClauses['GROUP BY'][0]
                # print("mongoGroup", mongoGroup)

                mongoWhere = "{}"
                if len(mongoClauses['WHERE']) != 0:
                    whereList = mongoClauses['WHERE']
                    mongoWhere = mongoWherer(whereList[0])
                    print(mongoWhere)

                mongoHaving = "{}"
                if len(mongoClauses['HAVING']) != 0:
                    havingList = mongoClauses['HAVING']
                    # print(havingList[0])
                    mongoHaving = mongoWherer(havingList[0])
                    print(mongoHaving)


                mongoOrder = ""
                if len(mongoClauses['ORDER BY']) != 0:
                    orderList = mongoClauses['ORDER BY'][0]
                    if " " in orderList:
                        orderParts = orderList.split(" ")
                        # print(orderParts)
                        if orderParts[1] == "ASC":
                            mongoOrder = orderParts[0] + ": 1"
                        else:
                            mongoOrder = orderParts[0] + ": -1"
                    else:
                        mongoOrder = mongoClauses['ORDER BY'][0] + ": 1"

                tempAggProj = mongoAggregation.split('"')[0]
                mongoProjection = mongoProjection + ' "' + tempAggProj + '": 1'
                print(mongoProjection)
                if len(mongoClauses['HAVING']) == 0:
                    mongoStatement = ("db." + mongoTable + ".aggregate([{$match: " + mongoWhere + '}, {$group: {_id: "$' + mongoGroup +
                                    '", "' + mongoAggregation + "}, {$project: {" + mongoProjection + "}}, {$match: " +
                                    mongoHaving + "}, {$sort: {" + mongoOrder + "}}])")
                else:
                    # print("mongoGroup", mongoGroup)
                    print("mongoAggregation", mongoAggregation)
                    mongoStatement = ("db." + mongoTable + ".aggregate([{$match: " + mongoWhere + '}, {$group: {_id: "$' + mongoGroup +
                                '", "' + mongoAggregation + "}}, {$project: {" + mongoProjection + "}}, {$match: " +
                                mongoHaving + "}, {$sort: {" + mongoOrder + "}}])")

            elif aggStatement == True:
                print("agg")

                selectionList = mongoClauses['SELECT']
                mongoAggregation = mongoAgger(selectionList)
                # print("jeer")
                mongoAggProj = mongoAggregation.split('"')[0]
                print("mongoAggProj", mongoAggProj)
                # print(selectionList)
                mongoProjection = mongoAggProjector(selectionList)
                aggIn = False
                for item in aggList:
                    # print(item)
                    if item in tokens:
                        print(item)
                        aggIn = True

                # print(mongoProjection)

                mongoWhere = ""
                if len(mongoClauses['WHERE']) != 0:
                    whereList = mongoClauses['WHERE']
                    mongoWhere = mongoWherer(whereList[0])
                    print(mongoWhere)

                mongoOrder = ""
                if len(mongoClauses['ORDER BY']) != 0:
                    orderList = mongoClauses['ORDER BY'][0]
                    if " " in orderList:
                        orderParts = orderList.split(" ")
                        # print(orderParts)
                        if orderParts[1] == "ASC":
                            mongoOrder = orderParts[0] + ": 1"
                        else:
                            mongoOrder = orderParts[0] + ": -1"
                    else:
                        mongoOrder = mongoClauses['ORDER BY'][0] + ": 1"

                    # print(mongoOrder)


                if mongoWhere != "":
                    if mongoOrder != "":
                        print("hererererer")
                        mongoStatement = ("db." + mongoTable + ".aggregate([{$match: " + mongoWhere + '}, {$group: {_id: null, "' +
                                            mongoAggregation + '}, {$project: { "_id": 0, "' + mongoAggProj  + '": 1}}, {$sort: {' + mongoOrder + "}}])")
                    else:
                        print("hererererer2")
                        mongoStatement = ("db." + mongoTable + ".aggregate([{$match: " + mongoWhere + '}, {$group: {_id: null, "' +
                                            mongoAggregation + '}}, {$project: { "_id": 0, "' + mongoAggProj  + '": 1}}])')
                else:
                    if mongoOrder != "":
                        print("hererererer3")
                        mongoStatement = ("db." + mongoTable + '.aggregate([{$group: {_id: null, "' + mongoAggregation +
                                        '}}, {$project: { "_id": 0, "' + mongoAggProj  + '": 1}}, {$sort: {" + mongoOrder + "}}])')
                    else:
                        print(mongoProjection)
                        mongoStatement = ("db." + mongoTable + '.aggregate([{$group: {_id: null, "' + mongoAggregation +
                                        '}}, {$project: { "_id": 0, "' + mongoAggProj  + '": 1}}])')

            else:
                print("no group and no agg")

                if mongoClauses['SELECT'][0] != "*":
                    selectionList = mongoClauses['SELECT']
                    mongoProjection = mongoSelector(selectionList)
                    print(mongoProjection)
                else:
                    mongoProjection = ""

                mongoWhere = ""
                if len(mongoClauses['WHERE']) != 0:
                    whereList = mongoClauses['WHERE']
                    mongoWhere = mongoWherer(whereList[0])
                    print(mongoWhere)

                mongoOrder = ""
                if len(mongoClauses['ORDER BY']) != 0:
                    orderList = mongoClauses['ORDER BY'][0]
                    if " " in orderList:
                        orderParts = orderList.split(" ")
                        # print(orderParts)
                        if orderParts[1] == "ASC":
                            mongoOrder = ".sort({ " + orderParts[0] + ": 1})"
                        else:
                            mongoOrder = ".sort({ " + orderParts[0] + ": -1})"
                    else:
                        mongoOrder = ".sort({ " + mongoClauses['ORDER BY'][0] + ": 1})"

                    print(mongoOrder)

                mongoStatement = "db." + mongoTable + ".find({" + mongoWhere + "}, {" + mongoProjection + "})" + mongoOrder

            print(mongoClauses)
            return mongoStatement

        return statement + ';'


    elif "join" in tokens:

        # print("join is in")
        print("filtered clauses:", filteredClauses)
        selections = selector(filteredClauses)
        print("selections:", selections)

        if orderIn == False:
            if len(filteredClauses) == 3:
                joiningWord = joinWorder(filteredClauses[1])
                statement = "SELECT " + selections + " FROM " + filteredClauses[1][0] + joiningWord + filteredClauses[1][-1] + " ON " + \
                        filteredClauses[1][0] + "." + filteredClauses[2][0] + " " + comparisonConverter(comparisons[0]) + " " + \
                        filteredClauses[1][-1] + "." + filteredClauses[2][-1]
            elif len(filteredClauses) == 4:
                joiningWord = joinWorder(filteredClauses[1])
                statement = ("SELECT " + selections + " FROM " + filteredClauses[1][0] + joiningWord + filteredClauses[1][-1] + \
                        " ON " + filteredClauses[1][0] + "." + filteredClauses[2][0] + " " + comparisonConverter(comparisons[0]) + \
                        " " + filteredClauses[1][-1] + "." + filteredClauses[2][-1]) + " WHERE " + filteredClauses[-1][0] + " " + \
                        comparisonConverter(comparisons[-1]) + " " + numbers[0]

        else:
            # print("order in")
            if len(filteredClauses) == 4:
                joiningWord = joinWorder(filteredClauses[1])
                statement = "SELECT " + selections + " FROM " + filteredClauses[1][0] + joiningWord + filteredClauses[1][-1] + " ON " + \
                        filteredClauses[1][0] + "." + filteredClauses[2][0] + " " + comparisonConverter(comparisons[0]) + " " + \
                        filteredClauses[1][-1] + "." + filteredClauses[2][-1] + " ORDER BY " + targets[-1] + order
            elif len(filteredClauses) == 5:
                joiningWord = joinWorder(filteredClauses[1])
                statement = ("SELECT " + selections + " FROM " + filteredClauses[1][0] + joiningWord + filteredClauses[1][-1] + \
                        " ON " + filteredClauses[1][0] + "." + filteredClauses[2][0] + " " + comparisonConverter(comparisons[0]) + \
                        " " + filteredClauses[1][-1] + "." + filteredClauses[2][-1]) + " WHERE " + filteredClauses[-1][0] + " " + \
                        comparisonConverter(comparisons[-1]) + " " + numbers[0] + " ORDER BY " + targets[-1] + order

        if db == "mongo":
            print(statement)
            mongoClauses = mongoJoinSplitter(statement)
            print(mongoClauses)

            mongoStatement = ''

            selectionList = mongoClauses['SELECT']
            # print("jeer")
            mongoProjection = mongoAggProjector(selectionList)
            # print(mongoProjection)

            mongoTable = mongoClauses['FROM'][0]
            print(mongoTable)

            joinParts = mongoClauses['JOIN'][0].split(" ")
            print(joinParts)

            mongoWhere = "{}"
            if len(mongoClauses['WHERE']) != 0:
                whereList = mongoClauses['WHERE']
                mongoWhere = mongoWherer(whereList[0])
                print(mongoWhere)

            mongoOrder = ""
            if len(mongoClauses['ORDER BY']) != 0:
                orderList = mongoClauses['ORDER BY'][0]
                if " " in orderList:
                    orderParts = orderList.split(" ")
                    # print(orderParts)
                    if orderParts[1] == "ASC":
                        mongoOrder = orderParts[0] + ": 1"
                    else:
                        mongoOrder = orderParts[0] + ": -1"
                else:
                    mongoOrder = mongoClauses['ORDER BY'][0] + ": 1"

            if len(mongoClauses['WHERE']) != 0:

                # print(mongoClauses['WHERE'][0])
                if len(mongoClauses['ORDER BY']) != 0:
                    # print(mongoClauses['ORDER BY'][0])
                    statement = "db." + mongoTable + ".aggregate([{$lookup: {from: '" + joinParts[0] + "', localField: '" + \
                            filteredClauses[2][0] + "', foreignField: '" + filteredClauses[2][-1] + "', as: 'foreignTable'}}, {$match: " + \
                            mongoWhere + "}, {$project: {" + mongoProjection + "}}, {$sort: {" + mongoOrder + "}}])"
                else:
                    print("where no order")
                    statement = "db." + mongoTable + ".aggregate([{$lookup: {from: '" + joinParts[0] + "', localField: '" + \
                                filteredClauses[2][0] + "', foreignField: '" + filteredClauses[2][-1] + "', as: 'foreignTable'}}, {$match: " + \
                                mongoWhere + "}, {$project: {" + mongoProjection + "}}])"
            else:
                if len(mongoClauses['ORDER BY']) != 0:
                    print("just order")
                    statement = "db." + mongoTable + '.aggregate([{$lookup: {from: "' + joinParts[0] + '", localField: "' + \
                                filteredClauses[2][0] + '", foreignField: "' + filteredClauses[2][-1] + '", as: "foreignTable"}}, ' + \
                                '{$project: {' + mongoProjection + '}}, {$sort: {' + mongoOrder + '}}])'
                else:
                    print("no where or order")
                    statement = "db." + mongoTable + '.aggregate([{$lookup: {from: "' + joinParts[0] + '", localField: "' + \
                                filteredClauses[2][0] + '", foreignField: "' + filteredClauses[2][-1] + '", as: "foreignTable"}}, ' + \
                                '{$project: {' + mongoProjection + '}}])'
        return statement


# while True:
#     sentence = input("enter query: ")

#     statement = translate_query(sentence, db)

#     print(statement)