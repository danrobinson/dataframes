import * as cTable from 'console.table'
import { readFileSync } from 'fs'
import { parse, unparse } from 'papaparse'
import { Z_FILTERED } from 'zlib'

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
    private _groups: string[] = [],
    _columnNames?: string[]
  ) {
    // use or infer columnNames
    if (_columnNames !== undefined) {
      this.columnNames = _columnNames
    } else if (_columns !== undefined) {
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
    return new Dataframe(undefined, rows)
  }

  public toCSV() {
    return unparse(this.rows())
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
            .indexOf(sourceName)
          if (index === -1) {
            throw new Error(`no column with name: ${sourceName}`)
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
        acc = {}
        for (const groupColumn of this._groups) {
          acc[groupColumn] = row[groupColumn]
        }
        for (const key in initial) {
          if (initial.hasOwnProperty(key)) {
            acc[key] = initial[key]
          }
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

  public getIndex(getKey: (row: any) => string): Map<string, any[]> {
    const keyed = new Map<string, any[]>()
    for (const row of this.rows()) {
      const key = getKey(row)
      const matchingRows = keyed.get(key)
      if (matchingRows === undefined) {
        keyed.set(key, [row])
      } else {
        matchingRows.push(row)
      }
    }
    return keyed
  }

  private join(
    other: Dataframe,
    joinType: 'left' | 'right' | 'inner' | 'full',
    options?: string | string[] | { [s: string]: string }
  ) {
    const newRows: any[] = []
    let getOwnKey: (row: any) => string
    let getOtherKey: (row: any) => string
    let newColumnNames: string[] = []
    if (options === undefined) {
      // natural join
      const otherNames = other.columnNames
      const commonNames = this.columnNames.filter(
        name => otherNames.indexOf(name) !== -1
      )
      const rightNames = otherNames.filter(
        name => this.columnNames.indexOf(name) === -1
      )
      newColumnNames = [...this.columnNames, ...rightNames]
      getOwnKey = row => JSON.stringify(commonNames.map(key => row[key]))
      getOtherKey = getOwnKey
    } else if (typeof options === 'string') {
      getOwnKey = row => row[options]
      getOtherKey = getOwnKey
    } else if (Array.isArray(options)) {
      getOwnKey = row => JSON.stringify(options.map(key => row[key]))
      getOtherKey = getOwnKey
    } else if (typeof options === 'object') {
      const ownKeys: string[] = []
      const otherKeys: string[] = []
      for (const key in options) {
        if (options.hasOwnProperty(key)) {
          ownKeys.push(key)
          otherKeys.push(options[key])
        }
      }
      getOwnKey = row => JSON.stringify(ownKeys.map(key => row[key]))
      getOtherKey = row => JSON.stringify(otherKeys.map(key => row[key]))
    } else {
      throw new Error('not yet supported')
    }
    if (joinType !== 'right') {
      const otherIndex = other.getIndex(getOtherKey)
      for (const row of this.rows()) {
        const key = getOwnKey(row)
        const matches = otherIndex.get(key)
        if (matches === undefined) {
          if (joinType === 'left' || joinType === 'full') {
            const newRow = { ...row }
            newRows.push(newRow)
          }
        } else {
          for (const match of matches) {
            const newRow = {
              ...row,
              ...match,
            }
            newRows.push(newRow)
          }
        }
      }
    }
    if (joinType === 'full' || joinType === 'right') {
      const thisIndex = this.getIndex(getOwnKey)
      for (const row of other.rows()) {
        const key = getOtherKey(row)
        const matches = thisIndex.get(key)
        if (matches === undefined) {
          const newRow = { ...row }
          newRows.push(newRow)
        } else if (joinType === 'right') {
          for (const match of matches) {
            const newRow = {
              ...match,
              ...row,
            }
            newRows.push(newRow)
          }
        }
      }
    }
    return new Dataframe(undefined, newRows, undefined, newColumnNames)
  }

  public leftJoin(
    other: Dataframe,
    options?: string | { [s: string]: string }
  ) {
    return this.join(other, 'left', options)
  }

  public rightJoin(
    other: Dataframe,
    options?: string | { [s: string]: string }
  ) {
    return this.join(other, 'right', options)
  }

  public innerJoin(
    other: Dataframe,
    options?: string | { [s: string]: string }
  ) {
    return this.join(other, 'inner', options)
  }

  public fullJoin(
    other: Dataframe,
    options?: string | { [s: string]: string }
  ) {
    return this.join(other, 'full', options)
  }
}
