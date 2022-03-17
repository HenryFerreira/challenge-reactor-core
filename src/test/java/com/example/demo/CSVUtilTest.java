package com.example.demo;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CSVUtilTest {

    @Test
    void converterData() {
        List<Player> list = CsvUtilFile.getPlayers();
        assert list.size() == 18207;
    }

    @Test
    void stream_filtrarJugadoresMayoresA35() {
        List<Player> list = CsvUtilFile.getPlayers();
        Map<String, List<Player>> listFilter = list.parallelStream()
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .flatMap(playerA -> list.parallelStream()
                        .filter(playerB -> playerA.club.equals(playerB.club))
                )
                .distinct()
                .collect(Collectors.groupingBy(Player::getClub));

        assert listFilter.size() == 322;
    }


    @Test
    void reactive_filtrarJugadoresMayoresA35() {
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .buffer(100)
                .flatMap(playerA -> listFlux
                        .filter(playerB -> playerA.stream()
                                .anyMatch(a -> a.club.equals(playerB.club)))
                )
                .distinct()
                .collectMultimap(Player::getClub);

        assert listFilter.block().size() == 322;
    }

    @Test
    void filtrarPorRankingDeVictoria() {

        //VARIABLES ----------------------------------------------------------//
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        //VARIABLES ----------------------------------------------------------//


        //FILTRO -------------------------------------------------------------//
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .buffer(100)
                .flatMap(playerA -> listFlux
                        .filter(playerB -> playerA.stream()
                                .anyMatch(a -> a.national.equals(playerB.national)))
                ).distinct()
                .sort((k, player) -> player.winners)
                .collectMultimap(Player::getNational);

        System.out.println("Por Nacionalidad: [" + listFilter.block().size() + "]");
        listFilter.block().forEach((pais, players) -> {
            System.out.println("\n[Pais: " + pais + "]");
            players.forEach(player -> {
                System.out.println("Nombre: ["+player.name + "] victorias: [" + player.winners + "]");
            });
        });
        //FILTRO -------------------------------------------------------------//
    }

    @Test
    void filtrarJugadoresMayoresa34() {
        //VARIABLES ----------------------------------------------------------//
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        //VARIABLES ----------------------------------------------------------//

        //FILTRO -------------------------------------------------------------//
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.age >= 34 && player.club.equals("FC Schalke 04"))
                .distinct()
                .collectMultimap(Player::getClub);
        System.out.println("--------------------------------------------");
        System.out.print("[Equipo: ");
        listFilter.block().forEach((equipo, players) -> {
            System.out.println(equipo + "]\n");
            players.forEach(player -> {
                System.out.println("[Nombre: " + player.name + "] [Edad: " + player.age + "]");
                assert player.club.equals("FC Schalke 04");
            });
        });
        System.out.println("--------------------------------------------");
        System.out.println(listFilter.block().size());
        assert listFilter.block().size() == 1;
        //FILTRO -------------------------------------------------------------//
    }


}
