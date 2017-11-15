/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "executors/commontableexecutor.h"
#include "storage/tableiterator.h"

namespace voltdb {

bool CommonTableExecutor::p_init(AbstractPlanNode*,
                                 const ExecutorVector& executorVector) {
    setTempOutputTable(executorVector);
    return true;
}

bool CommonTableExecutor::p_execute(const NValueArray& params) {
    AbstractTempTable* inputTable = m_abstractNode->getTempInputTable();
    AbstractTempTable* outputTable = m_abstractNode->getTempInputTable();

    TableTuple iterTuple(inputTable->schema());
    TableIterator iter = inputTable->iterator();
    while (iter.next(iterTuple)) {
        outputTable->insertTuple(iterTuple);
    }

    return true;
}


} // end namespace voltdb
