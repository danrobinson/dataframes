import * as cTable from 'console.table'
import { readFileSync } from 'fs'
import { parse } from 'papaparse'

export interface Vector {
  name: string
  values: any[]
}

// summarizes

export interface Summarization {
  init: any
  step: (previousValue, nextValue) => any
  result: (result: any) => any
}

export function isSummarization(arg: any): arg is Summarization {
  if (typeof arg !== 'object') {
    return false
  } else {
    return (
      arg.init !== undefined &&
      arg.step !== undefined &&
      arg.result !== undefined
    )
  }
}

export type Mapping = (row: any) => string | number

const id = a => a

export function sum(columnName: string): Summarization {
  return {
    init: 0,
    step: (previousValue, currentValue) =>
      previousValue + currentValue[columnName],
    result: id,
  }
}

export function count(): Summarization {
  return {
    init: 0,
    step: (previousValue, currentValue) => previousValue + 1,
    result: id,
  }
}

export function mean(columnName: string): Summarization {
  return {
    init: {
      count: 0,
      sum: 0,
    },
    step: (previousValue, currentValue) => ({
      count: previousValue.count + 1,
      sum: previousValue.sum + currentValue[columnName],
    }),
    result: (res: { count: number; sum: number }) => res.sum / res.count,
  }
}

export class Dataframe {
  public columnNames: string[]

  constructor(
    private _columns?: Vector[],
    private _rows?: object[],
    private _groups: string[] = []
  ) {
    if (_columns !== undefined) {
      this.columnNames = _columns.map(column => column.name)
    } else if (_rows !== undefined) {
      const columnNameSet: Set<string> = new Set()
      _rows.map(row => Object.keys(row).map(name => columnNameSet.add(name)))
      this.columnNames = Array.from(columnNameSet)
    } else {
      throw new Error('either columns or rows must be passed')
    }
  }

  public static fromCSV(path: string) {
    const text = readFileSync(path, { encoding: 'utf-8' })
    const rows = parse(text, { header: true, dynamicTyping: true }).data
    console.log('rows', rows)
    return new Dataframe(undefined, rows)
    // console.log(
    //   parse(text, {}, (err, output) => {
    //     console.log(output)
    //   })
    // )
  }

  public table() {
    return cTable.getTable(this.rows())
  }

  public columns(): Vector[] {
    if (this._columns === undefined) {
      if (this._rows === undefined) {
        throw new Error('dataframe unexpectedly has no values')
      }
      const rows = this._rows
      this._columns = this.columnNames.map(name => ({
        name,
        values: rows.map(row => row[name]),
      }))
    }
    return this._columns
  }

  public rows(): object[] {
    if (this._rows === undefined) {
      const columns = this._columns
      if (columns === undefined) {
        throw new Error('dataframe unexpectedly has no values')
      }
      this._rows = columns[0].values.map((_, rowIndex) => {
        const obj = {}
        this.columnNames.map((name, colIndex) => {
          obj[name] = columns[colIndex].values[rowIndex]
        })
        return obj
      })
    }
    return this._rows
  }

  public count(): number {
    if (this._columns !== undefined) {
      return this._columns[0].values.length
    } else if (this._rows !== undefined) {
      return this._rows.length
    } else {
      throw new Error('dataframe unexpectedly has no data')
    }
  }

  public select(arg: string | string[] | { [s: string]: string }): Dataframe {
    if (Array.isArray(arg)) {
      return new Dataframe(
        this.columns().filter(column => arg.indexOf(column.name) !== -1)
      )
    } else if (typeof arg === 'object') {
      const newColumns: Vector[] = []
      for (const targetName in arg) {
        if (arg.hasOwnProperty(targetName)) {
          const sourceName = arg[targetName]
          const index = this.columns()
            .map(column => column.name)
            .indexOf(targetName)
          if (index === -1) {
            throw new Error(`no column with name: ${targetName}`)
          }
          const oldColumn = this.columns()[index]
          newColumns.push({ name: targetName, values: oldColumn.values })
        }
      }
      return new Dataframe(newColumns)
    } else {
      return new Dataframe(this.columns().filter(column => column.name === arg))
    }
  }

  public filter(func: (row: any) => boolean): Dataframe {
    const newRows = this.rows().filter(func)
    return new Dataframe(undefined, newRows)
  }

  public mutate(fields: { [s: string]: Summarization | Mapping }) {
    // one at a time
    if (Object.keys(fields).length === 0) {
      return this
    }
    const key = Object.keys(fields)[0]
    if (this.columnNames.indexOf(key) !== -1) {
      throw new Error(`cannot redefine column ${key}`)
    }
    const mutator = fields[key]
    let newVector: Vector
    if (isSummarization(mutator)) {
      const groups = this._summarize({ [key]: mutator })
      if (this._groups.length === 0) {
        newVector = {
          name: key,
          values: Array(this.count()).fill(groups.get('[]')[key]),
        }
      } else {
        const values = this.rows().map(
          row => groups.get(this.getGroupKey(row))[key]
        )
        newVector = { name: key, values }
      }
    } else {
      newVector = { name: key, values: this.rows().map(mutator) }
    }
    const newFields = { ...fields }
    delete newFields[key]
    return new Dataframe([...this.columns(), newVector]).mutate(newFields)
  }

  public sort(arg: string | string[]) {
    const keys = (Array.isArray(arg) ? arg : [arg]).map(key => {
      if (key[0] === '-') {
        return {
          key: key.slice(1),
          order: -1,
        }
      } else {
        return { key, order: 1 }
      }
    })
    const newRows = [...this.rows()] // make copy
    newRows.sort((a, b) => {
      for (const key of keys) {
        if (a[key.key] > b[key.key]) {
          return 1 * key.order
        } else if (a[key.key] < b[key.key]) {
          return -1 * key.order
        }
      }
      return 0
    })
    return new Dataframe(undefined, newRows)
  }

  private getGroupKey(row: object): string {
    return JSON.stringify(this._groups.map(group => row[group]))
  }

  private _summarize(arg: { [s: string]: Summarization }) {
    const keys = Object.keys(arg)
    const initial = {}
    keys.map(key => (initial[key] = arg[key].init))
    const rows = this.rows()
    const groups = new Map<string, any>()
    for (const row of rows) {
      const groupKey = this.getGroupKey(row)
      let acc = groups.get(groupKey)
      if (acc === undefined) {
        acc = { ...initial }
        for (const groupColumn of this._groups) {
          acc[groupColumn] = row[groupColumn]
        }
      }
      for (const key of keys) {
        acc[key] = arg[key].step(acc[key], row)
      }
      groups.set(groupKey, acc)
    }
    for (const acc of groups.values()) {
      for (const key of keys) {
        acc[key] = arg[key].result(acc[key])
      }
    }
    return groups
  }

  public summarize(arg: { [s: string]: Summarization }) {
    const groups = this._summarize(arg)
    const newGroups = [...this._groups]
    newGroups.pop()
    return new Dataframe(undefined, Array.from(groups.values()), newGroups)
  }

  public groupBy(arg: string | string[]) {
    const keys = Array.isArray(arg) ? arg : [arg]
    keys.map(key => {
      if (this.columnNames.indexOf(key) === -1) {
        throw new Error(`no column in dataframe named ${key}`)
      }
      if (this._groups.indexOf(key) !== -1) {
        throw new Error(`dataframe is already grouped by key ${key}`)
      }
    })
    return new Dataframe(this._columns, this._rows, [...this._groups, ...keys])
  }
}
