.PHONY: composer-update
composer-update: ## Update project dependencies
		docker run --rm --interactive --tty --volume $(PWD):/app composer update

.PHONY: phpunit
phpunit: ## execute project unit tests
		docker run -it --rm --volume $(PWD):/app -w /app php:7.4-cli vendor/bin/phpunit $(conf)

