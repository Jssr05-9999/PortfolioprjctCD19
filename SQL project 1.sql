/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (1000) [iso_code]
      ,[continent]
      ,[location]
      ,[date]
      ,[population]
      ,[total_cases]
      ,[new_cases]
      ,[new_cases_smoothed]
      ,[total_deaths]
      ,[new_deaths]
      ,[new_deaths_smoothed]
      ,[total_cases_per_million]
      ,[new_cases_per_million]
      ,[new_cases_smoothed_per_million]
      ,[total_deaths_per_million]
      ,[new_deaths_per_million]
      ,[new_deaths_smoothed_per_million]
      ,[reproduction_rate]
      ,[icu_patients]
      ,[icu_patients_per_million]
      ,[hosp_patients]
      ,[hosp_patients_per_million]
      ,[weekly_icu_admissions]
      ,[weekly_icu_admissions_per_million]
      ,[weekly_hosp_admissions]
      ,[weekly_hosp_admissions_per_million]
  FROM [portfolioproject].[dbo].[CovidDeaths$]

  select*
  from CovidDeaths$
  order by 1,2

  select*
  from CovidVacci
  order by 1,2

  select location, date, total_cases, new_cases, total_deaths, population
  from CovidDeaths$
  order by 1,2

  --looking at total cases vs total deaths

  select location, date, total_cases, total_deaths, (total_cases/total_deaths)*100 as deathpercentage
  from CovidDeaths$
  order by 1,2 

  select location, date, total_cases, total_deaths, (total_cases/total_deaths)*100 as deathpercentage
  from CovidDeaths$
  where location like '%states%'
  order by 1,2 

  --looking at totalcases vs population
--whatcountries have the highest infection rates compared to population 

  select location, date,population, total_cases,(total_cases/population)*100 as deathpercentage
  from CovidDeaths$
  where location like '%states%'
  order by 1,2 

  --looking at countries with highest infection rates compared to the population 

  select location,population,max(total_cases) as highestinfectioncount, max(total_cases/population)*100 as percentpopulationinfected
  from CovidDeaths$
  --where location like '%states%'
  group by location, population
  order by 1,2

  select location,population,max(total_cases) as highestinfectioncount, max(total_cases/population)*100 as percentpopulationinfected
  from CovidDeaths$
  --where location like '%states%'
  group by location, population
  order by percentpopulationinfected desc

  --showing countries with highest deaths count for population

  select location,max(cast(total_deaths as int)) as totaldeathcount
  from CovidDeaths$
  --where location like '%states%'
  group by location
  order by totaldeathcount desc

  --showing countries with highest death count for population 

  select location,max(cast(total_deaths as int)) as totaldeathcount
  from CovidDeaths$
  where continent is not null
  group by location
  order by totaldeathcount desc

  select location,max(cast(total_deaths as int)) as totaldeathcount
  from CovidDeaths$
  where continent is null
  group by location
  order by totaldeathcount desc

  --GLOBAL NUMBERS
--this shows the each day the total cass recorded across the world

  select date, sum(new_cases)
  from CovidDeaths$
  where continent is null
  group by date
  order by 1,2

SELECT date, sum(new_cases),sum(cast(new_deaths as int)),sum(cast(new_deaths as int))/sum(new_cases)*100 as deathpercentage
from CovidDeaths$
where continent is not null
group by date
order by 1,2

SELECT date, sum(new_cases) as total_cases,sum(cast(new_deaths as int)) as total_deaths,sum(cast(new_deaths as int))/sum(new_cases)*100 as deathpercentage
from CovidDeaths$
where continent is not null
group by date
order by 1,2

--this will give the total cases, total deaths and the death percentage

SELECT sum(new_cases) as total_cases,sum(cast(new_deaths as int)) as total_deaths,sum(cast(new_deaths as int))/sum(new_cases)*100 as deathpercentage
from CovidDeaths$
where continent is not null
order by 1,2

--Now we are using the another table covidvacci
--join the both tables

select*
from CovidVacci

select*
from CovidVacci as CV
join CovidDeaths$ as CD
on cv.location=CD.location
and CV.date=CD.date

--this shows the total amount of people in the world that have been vaccinated

select CD.continent, CD.location, CD.date, CD.population, CV.new_vaccinations
from CovidVacci as CV
join CovidDeaths$ as CD
on cv.location=CD.location
and CV.date=CD.date
where CD.continent is not null
order by 2,3

--here we are used rolling count and
--using partition by windows function

select CD.continent, CD.location, CD.date, CD.population, CV.new_vaccinations,sum(cast(new_vaccinations as int)) over(partition by CD.location order by CD.location,CD.date) 
from CovidVacci as CV
join CovidDeaths$ as CD
on cv.location=CD.location
and CV.date=CD.date
where CD.continent is not null
order by 2,3

select CD.continent, CD.location, CD.date, CD.population, CV.new_vaccinations,sum(cast(new_vaccinations as int)) over(partition by CD.location order by CD.location,CD.date)
--as rollingpeoplevaccinated, (rollingpeoplevaccinated/population)*100
from CovidVacci as CV
join CovidDeaths$ as CD
on cv.location=CD.location
and CV.date=CD.date
where CD.continent is not null
order by 2,3

--USE CTE Table

with popvsvac (continent,location,date,population,new_vaccinations,rollingpeoplevaccinated)
as
(
select CD.continent, CD.location, CD.date, CD.population, CV.new_vaccinations,sum(cast(new_vaccinations as int)) over(partition by CD.location order by CD.location,CD.date)
as rollingpeoplevaccinated
--,(rollingpeoplevaccinated/population)*100
from CovidVacci as CV
join CovidDeaths$ as CD
on cv.location=CD.location
and CV.date=CD.date
where CD.continent is not null
)
select*,(rollingpeoplevaccinated/population)*100
from popvsvac

--USE TEMP TABLE


CREATE TABLE #percentpopulationvaccinated
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
new_vaccinations numeric,
rollingpeoplevaccinated numeric
)
insert into #percentpopulationvaccinated
select CD.continent, CD.location, CD.date, CD.population, CV.new_vaccinations,sum(cast(new_vaccinations as int)) over(partition by CD.location order by CD.location,CD.date)
as rollingpeoplevaccinated
--,(rollingpeoplevaccinated/population)*100
from CovidVacci as CV
join CovidDeaths$ as CD
on cv.location=CD.location
and CV.date=CD.date
where CD.continent is not null

select*, (rollingpeoplevaccinated/population)*100
from #percentpopulationvaccinated



Drop table if exists #percentpopulationvaccinated
CREATE TABLE #percentpopulationvaccinated
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
new_vaccinations numeric,
rollingpeoplevaccinated numeric
)
insert into #percentpopulationvaccinated
select CD.continent, CD.location, CD.date, CD.population, CV.new_vaccinations,sum(cast(new_vaccinations as int)) over(partition by CD.location order by CD.location,CD.date)
as rollingpeoplevaccinated
--,(rollingpeoplevaccinated/population)*100
from CovidVacci as CV
join CovidDeaths$ as CD
on cv.location=CD.location
and CV.date=CD.date
where CD.continent is not null

select*, (rollingpeoplevaccinated/population)*100
from #percentpopulationvaccinated


--creating view is to store data for later visualisation

create view percentpopulationvaccinated as
select CD.continent, CD.location, CD.date, CD.population, CV.new_vaccinations,sum(cast(new_vaccinations as int)) over(partition by CD.location order by CD.location,CD.date)
as rollingpeoplevaccinated
--,(rollingpeoplevaccinated/population)*100
from CovidVacci as CV
join CovidDeaths$ as CD
on cv.location=CD.location
and CV.date=CD.date
where CD.continent is not null

select*, (rollingpeoplevaccinated/population)*100
from #percentpopulationvaccinated