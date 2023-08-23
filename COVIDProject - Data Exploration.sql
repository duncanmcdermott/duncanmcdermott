/*
Covid 19 Data Exploration 

Skills used: Joins, CTE's, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types

*/

Select *
From COVIDProject..CovidDeaths
Where continent is not null 
order by 3,4

-- Change certain strings to integers for further mathematical operations

ALTER TABLE COVIDProject..CovidDeaths
ALTER COLUMN total_cases INT

ALTER TABLE COVIDProject..CovidDeaths
ALTER COLUMN total_deaths INT

-- Select Data that we are going to be starting with


Select Location, date, total_cases, new_cases, total_deaths, population
From COVIDProject..CovidDeaths
Where continent is not null 
order by 1,2

-- Looking at Total Cases vs Total Deaths
-- Shows likelihood of dying if you contract COVID in your country

Select Location, date, total_cases,total_deaths, (total_deaths/total_cases)*100 as DeathPercentage
From COVIDProject..CovidDeaths
Where location like '%states%'
and continent is not null 
order by 1,2

--Looking at Total Cases vs Population
--Shows what percentage of population infected with Covid across time

Select Location, date, Population, (total_cases/population)* 100 AS PercentPopulationInfected
From COVIDProject..CovidDeaths
order by 1,2


-- Countries with highest infection rates

Select Location, Population, MAX(total_cases) as HighestInfectionCount,  
	MAX((total_cases/population))*100 as PercentPopulationInfected
From COVIDProject..CovidDeaths
--Where location like '%states%'
Group by Location, Population
order by PercentPopulationInfected desc


-- Showing Countries with Highest Death Count per capita

Select Location, MAX(total_deaths) as TotalDeathCount
From COVIDProject..CovidDeaths
Where continent is not null
Group by Location
order by TotalDeathCount desc


-- Contintents with the highest death count

Select continent, MAX(Total_deaths) as TotalDeathCount
From COVIDProject..CovidDeaths
--Where location like '%states%'
Where continent is not null 
Group by continent
order by TotalDeathCount desc


-- Global Numbers

Select SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths,
	SUM(cast(new_deaths as int))/SUM(New_Cases)*100 as DeathPercentage
From COVIDProject..CovidDeaths
where continent is not null
order by 1,2


-- Total Population vs Vaccinations
-- Shows Percentage of Population that has recieved at least one Covid Vaccine


Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CAST(vac.new_vaccinations as bigint)) OVER (Partition by dea.location Order by dea.location, dea.date) 
as RollingPeopleVaccinated
from COVIDProject..CovidDeaths dea
Join COVIDProject..CovidVaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null
order by 2,3



-- Use CTE to perform Calculation on Partition By in previous query

With PopvsVac (Continent, Location, Date, Population, new_vaccinations, RollingPeopleVaccinated)
as
(
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CAST(vac.new_vaccinations as bigint)) OVER (Partition by dea.location Order by dea.location, dea.date) 
as RollingPeopleVaccinated
from COVIDProject..CovidDeaths dea
Join COVIDProject..CovidVaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null
--order by 2,3
)
Select *, (RollingPeopleVaccinated/Population)*100
From PopvsVac


-- Using Temp Table to perform Calculation on Partition By in previous query

DROP Table if exists #PercentPopulationVaccinated
Create Table #PercentPopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population numeric,
New_vaccinations numeric,
RollingPeopleVaccinated numeric
)

Insert into #PercentPopulationVaccinated
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From COVIDProject..CovidDeaths dea
Join COVIDProject..CovidVaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date
--where dea.continent is not null 
--order by 2,3

Select *, (RollingPeopleVaccinated/Population)*100
From #PercentPopulationVaccinated



-- Creating View to store data for later visualizations

Create View PercentPopulationVaccinated as
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From COVIDProject..CovidDeaths dea
Join COVIDProject..CovidVaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
--order by 2,3