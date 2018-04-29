import { expect } from 'chai'
import 'mocha'
import { Dataframe, mean, sum } from '../index'

describe('Dataframe', () => {
  const frame = Dataframe.fromCSV('./gapminder.csv').filter(
    row => row.country !== null && row.population !== null && row.life !== null
  )
  describe('columnNames', () => {
    it('should have the right properties', () => {
      expect(frame.columnNames).to.have.length(6)
      expect(frame.columnNames[0]).to.equal('country')
    })
  })
  describe('columns', () => {
    it('should have the right properties', () => {
      expect(frame.columns()).to.have.length(6)
      expect(frame.columns()[0].name).to.equal('country')
    })
  })
  describe('rows', () => {
    it('should have the right properties', () => {
      expect(frame.rows()).to.have.length(15467)
    })
  })
  describe('select', () => {
    it('should select a single column', () => {
      const selected = frame.select('country')
      expect(selected.columns().length).to.equal(1)
      expect(selected.columnNames.length).to.equal(1)
      expect(selected.columns()[0].name).to.equal('country')
    })
    it('should select multiple columns', () => {
      const selected = frame.select(['country', 'year'])
      expect(selected.columns().length).to.equal(2)
      expect(selected.columnNames.length).to.equal(2)
      expect(selected.columns()[0].name).to.equal('country')
      expect(selected.columns()[1].name).to.equal('year')
      expect(selected.columnNames[0]).to.equal('country')
      expect(selected.columnNames[1]).to.equal('year')
    })
  })
  describe('filter', () => {
    it('should be able to filter by year', () => {
      const filtered = frame.filter(row => row.year > 1900)
      expect(filtered.rows()).to.have.length(13388)
      expect(filtered.rows().every((row: any) => row.year > 1900)).to.equal(
        true
      )
    })
  })
  describe('sort', () => {
    it('should be able to sort by year', () => {
      const sorted = frame.sort('year')
      sorted.rows().reduce((previous: any, current: any) => {
        expect(previous.year <= current.year)
        return current
      })
    })
  })
  describe('mutate', () => {
    const mutated = frame.mutate({
      gdp: row => row.income * row.population,
    })
    it('should add a new column', () => {
      expect(mutated.columns()).to.have.length(7)
      expect(mutated.columnNames).to.have.length(7)
      expect(mutated.columns()[6].name).to.equal('gdp')
      expect(mutated.columnNames[6]).to.equal('gdp')
    })
  })
  describe('summarize', () => {
    const summarized = frame
      .groupBy(['country'])
      .summarize({ average_life: mean('life') })
    it('should be able to take the mean', () => {
      expect(summarized.columns()).to.have.length(2)
      expect(summarized.columnNames).to.have.length(2)
      expect(summarized.columns()[1].name).to.equal('average_life')
    })
  })
})
