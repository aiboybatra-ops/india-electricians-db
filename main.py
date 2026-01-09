#!/usr/bin/env python3
"""
Main orchestrator for the India Electricians Scraper.
Coordinates all scrapers and manages data collection across states and cities.
"""
import argparse
import logging
import sys
from datetime import datetime
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from src.config import INDIAN_LOCATIONS, OUTPUT_DIR, LOG_DIR
from src.models import Electrician, ScrapeResult
from src.storage import DataStorage
from src.scrapers.google_scraper import GoogleMapsScraper
from src.scrapers.justdial_scraper import JustDialScraper
from src.scrapers.indiamart_scraper import IndiaMARTScraper
from src.scrapers.sulekha_scraper import SulekhaScraper
from src.scrapers.urbancompany_scraper import UrbanCompanyScraper


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Set up logging configuration."""
    log_level = logging.DEBUG if verbose else logging.INFO
    
    # Create log file
    log_file = LOG_DIR / f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )
    
    return logging.getLogger("main")


class ScraperOrchestrator:
    """Orchestrates multiple scrapers to collect electrician data."""
    
    def __init__(
        self,
        scrapers: List[str] = None,
        states: List[str] = None,
        cities: List[str] = None,
        max_workers: int = 2,
        verbose: bool = False,
    ):
        self.logger = setup_logging(verbose)
        self.storage = DataStorage()
        self.max_workers = max_workers
        
        # Initialize scrapers
        self.available_scrapers = {
            "google": GoogleMapsScraper,
            "justdial": JustDialScraper,
            "indiamart": IndiaMARTScraper,
            "sulekha": SulekhaScraper,
            "urbancompany": UrbanCompanyScraper,
        }
        
        # Select scrapers to use
        if scrapers:
            self.scrapers = {
                name: cls() for name, cls in self.available_scrapers.items()
                if name in scrapers
            }
        else:
            self.scrapers = {
                name: cls() for name, cls in self.available_scrapers.items()
            }
        
        # Select locations
        self.locations = self._select_locations(states, cities)
        
        self.logger.info(f"Initialized with {len(self.scrapers)} scrapers")
        self.logger.info(f"Will scrape {sum(len(cities) for cities in self.locations.values())} cities")
    
    def _select_locations(
        self,
        states: List[str] = None,
        cities: List[str] = None,
    ) -> Dict[str, List[str]]:
        """Select locations to scrape based on filters."""
        if states:
            # Filter by specified states
            locations = {
                state: cities_list
                for state, cities_list in INDIAN_LOCATIONS.items()
                if state.lower() in [s.lower() for s in states]
            }
        else:
            locations = INDIAN_LOCATIONS.copy()
        
        if cities:
            # Further filter by specified cities
            cities_lower = [c.lower() for c in cities]
            locations = {
                state: [city for city in cities_list if city.lower() in cities_lower]
                for state, cities_list in locations.items()
            }
            # Remove empty states
            locations = {k: v for k, v in locations.items() if v}
        
        return locations
    
    def _scrape_location(
        self,
        scraper_name: str,
        city: str,
        state: str,
    ) -> ScrapeResult:
        """Scrape a single location with a specific scraper."""
        scraper = self.scrapers.get(scraper_name)
        if not scraper:
            return ScrapeResult(
                success=False,
                source=scraper_name,
                city=city,
                state=state,
                error_message=f"Scraper {scraper_name} not found",
            )
        
        try:
            result = scraper.scrape_city(city, state)
            return result
        except Exception as e:
            self.logger.error(f"Error scraping {city}, {state} with {scraper_name}: {e}")
            return ScrapeResult(
                success=False,
                source=scraper_name,
                city=city,
                state=state,
                error_message=str(e),
            )
    
    def run_sequential(self) -> List[ScrapeResult]:
        """Run scrapers sequentially (safer but slower)."""
        all_results = []
        
        total_tasks = sum(
            len(cities) * len(self.scrapers)
            for cities in self.locations.values()
        )
        
        with tqdm(total=total_tasks, desc="Scraping Progress") as pbar:
            for state, cities in self.locations.items():
                for city in cities:
                    for scraper_name in self.scrapers:
                        pbar.set_description(f"{scraper_name}: {city}, {state}")
                        
                        result = self._scrape_location(scraper_name, city, state)
                        all_results.append(result)
                        
                        if result.success and result.electricians:
                            # Save to database immediately
                            saved = self.storage.save_to_database(result.electricians)
                            self.logger.info(
                                f"Saved {saved} new electricians from {city}, {state}"
                            )
                        
                        pbar.update(1)
        
        return all_results
    
    def run_parallel(self) -> List[ScrapeResult]:
        """Run scrapers in parallel (faster but more resource intensive)."""
        all_results = []
        tasks = []
        
        # Create list of all tasks
        for state, cities in self.locations.items():
            for city in cities:
                for scraper_name in self.scrapers:
                    tasks.append((scraper_name, city, state))
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(self._scrape_location, *task): task
                for task in tasks
            }
            
            # Process completed tasks
            with tqdm(total=len(tasks), desc="Scraping Progress") as pbar:
                for future in as_completed(futures):
                    task = futures[future]
                    try:
                        result = future.result()
                        all_results.append(result)
                        
                        if result.success and result.electricians:
                            saved = self.storage.save_to_database(result.electricians)
                            self.logger.info(
                                f"Saved {saved} new electricians from {task[1]}, {task[2]}"
                            )
                    except Exception as e:
                        self.logger.error(f"Task {task} failed: {e}")
                        all_results.append(
                            ScrapeResult(
                                success=False,
                                source=task[0],
                                city=task[1],
                                state=task[2],
                                error_message=str(e),
                            )
                        )
                    
                    pbar.update(1)
        
        return all_results
    
    def run(self, parallel: bool = False) -> Dict:
        """Run the scraping process."""
        self.logger.info("Starting scraping process...")
        start_time = datetime.now()
        
        if parallel:
            results = self.run_parallel()
        else:
            results = self.run_sequential()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Generate summary
        successful = sum(1 for r in results if r.success)
        failed = len(results) - successful
        total_electricians = sum(len(r.electricians) for r in results if r.success)
        
        summary = {
            "total_tasks": len(results),
            "successful": successful,
            "failed": failed,
            "total_electricians_found": total_electricians,
            "duration_seconds": duration,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
        }
        
        self.logger.info(f"Scraping completed in {duration:.2f} seconds")
        self.logger.info(f"Total electricians found: {total_electricians}")
        self.logger.info(f"Success rate: {successful}/{len(results)}")
        
        return summary
    
    def export_results(self, format: str = "all") -> Dict[str, str]:
        """Export results to various formats."""
        exports = {}
        
        if format in ["all", "csv"]:
            csv_path = self.storage.save_to_csv(
                self.storage.load_from_database(),
                filename="electricians_export.csv",
                append=False,
            )
            exports["csv"] = csv_path
            self.logger.info(f"Exported to CSV: {csv_path}")
        
        if format in ["all", "json"]:
            json_path = self.storage.save_to_json(
                self.storage.load_from_database(),
                filename="electricians_export.json",
                append=False,
            )
            exports["json"] = json_path
            self.logger.info(f"Exported to JSON: {json_path}")
        
        if format in ["all", "excel"]:
            excel_path = self.storage.export_to_excel(
                filename="electricians_export.xlsx"
            )
            exports["excel"] = excel_path
            self.logger.info(f"Exported to Excel: {excel_path}")
        
        return exports
    
    def get_statistics(self) -> Dict:
        """Get statistics about collected data."""
        return self.storage.get_statistics()
    
    def close(self):
        """Clean up resources."""
        for scraper in self.scrapers.values():
            scraper.close()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Scrape electrician data from various Indian business directories"
    )
    
    parser.add_argument(
        "--scrapers",
        nargs="+",
        choices=["google", "justdial", "indiamart", "sulekha", "urbancompany"],
        help="Specific scrapers to use (default: all)",
    )
    
    parser.add_argument(
        "--states",
        nargs="+",
        help="Specific states to scrape (default: all)",
    )
    
    parser.add_argument(
        "--cities",
        nargs="+",
        help="Specific cities to scrape (default: all cities in selected states)",
    )
    
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Run scrapers in parallel (faster but more resource intensive)",
    )
    
    parser.add_argument(
        "--workers",
        type=int,
        default=2,
        help="Number of parallel workers (default: 2)",
    )
    
    parser.add_argument(
        "--export",
        choices=["csv", "json", "excel", "all"],
        default="all",
        help="Export format (default: all)",
    )
    
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    
    parser.add_argument(
        "--stats-only",
        action="store_true",
        help="Only show statistics, don't scrape",
    )
    
    args = parser.parse_args()
    
    # Initialize orchestrator
    orchestrator = ScraperOrchestrator(
        scrapers=args.scrapers,
        states=args.states,
        cities=args.cities,
        max_workers=args.workers,
        verbose=args.verbose,
    )
    
    try:
        if args.stats_only:
            # Just show statistics
            stats = orchestrator.get_statistics()
            print("\n=== Database Statistics ===")
            print(f"Total records: {stats['total_records']}")
            print("\nBy State:")
            for state, count in sorted(stats['by_state'].items(), key=lambda x: -x[1])[:10]:
                print(f"  {state}: {count}")
            print("\nBy Source:")
            for source, count in stats['by_source'].items():
                print(f"  {source}: {count}")
            print("\nTop Cities:")
            for city, count in stats['top_cities'].items():
                print(f"  {city}: {count}")
        else:
            # Run scraping
            summary = orchestrator.run(parallel=args.parallel)
            
            print("\n=== Scraping Summary ===")
            print(f"Total tasks: {summary['total_tasks']}")
            print(f"Successful: {summary['successful']}")
            print(f"Failed: {summary['failed']}")
            print(f"Total electricians found: {summary['total_electricians_found']}")
            print(f"Duration: {summary['duration_seconds']:.2f} seconds")
            
            # Export results
            exports = orchestrator.export_results(format=args.export)
            print("\n=== Exports ===")
            for format_name, path in exports.items():
                print(f"{format_name.upper()}: {path}")
            
            # Show final statistics
            stats = orchestrator.get_statistics()
            print(f"\nTotal records in database: {stats['total_records']}")
    
    finally:
        orchestrator.close()


if __name__ == "__main__":
    main()
