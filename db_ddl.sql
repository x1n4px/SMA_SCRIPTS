-- astro.Ecuacion_parametrica definition

CREATE TABLE `Ecuacion_parametrica` (
  `IdEc` int(11) NOT NULL,
  `a` decimal(21,18) DEFAULT NULL,
  `b` decimal(21,18) DEFAULT NULL,
  `c` decimal(21,18) DEFAULT NULL,
  `Inicio_Estacion_1` varchar(200) DEFAULT NULL,
  `Fin_Estacion_1` varchar(200) DEFAULT NULL,
  `Inicio_Estacion_2` varchar(200) DEFAULT NULL,
  `Fin_Estacion_2` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`IdEc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.Lluvia definition

CREATE TABLE `Lluvia` (
  `Identificador` varchar(20) NOT NULL,
  `Año` int(11) NOT NULL,
  `Nombre` varchar(200) DEFAULT NULL,
  `Fecha_Inicio` date DEFAULT NULL,
  `Fecha_Fin` date DEFAULT NULL,
  `Velocidad` int(11) DEFAULT NULL,
  PRIMARY KEY (`Identificador`,`Año`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.Meteoro definition

CREATE TABLE `Meteoro` (
  `Identificador` int(11) NOT NULL,
  `Fecha` date DEFAULT NULL,
  `Hora` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`Identificador`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.Observatorio definition

CREATE TABLE `Observatorio` (
  `Número` int(11) NOT NULL,
  `Nombre_Camara` varchar(200) DEFAULT NULL,
  `Descripción` varchar(200) DEFAULT NULL,
  `Longitud_Sexagesimal` varchar(200) DEFAULT NULL,
  `Latitud_Sexagesimal` varchar(200) DEFAULT NULL,
  `Longitud_Radianes` decimal(20,15) DEFAULT NULL,
  `Latitud_Radianes` decimal(20,15) DEFAULT NULL,
  `Altitud` int(11) DEFAULT NULL,
  `Directorio_Local` varchar(200) DEFAULT NULL,
  `Directorio_Nube` varchar(200) DEFAULT NULL,
  `Tamaño_Chip` int(11) DEFAULT NULL,
  `Orientación_Chip` int(11) DEFAULT NULL,
  `Máscara` varchar(200) DEFAULT NULL,
  `Créditos` varchar(200) DEFAULT NULL,
  `Nombre_Observatorio` varchar(200) DEFAULT NULL,
  `Activo` int(11) DEFAULT NULL,
  PRIMARY KEY (`Número`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.audit_log definition

CREATE TABLE `audit_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `event_type` varchar(255) NOT NULL,
  `user_id` int(11) DEFAULT NULL,
  `button_name` varchar(255) DEFAULT NULL,
  `report_id` int(11) DEFAULT NULL,
  `timestamp` datetime DEFAULT current_timestamp(),
  `isGuest` tinyint(1) DEFAULT 0,
  `event_target` varchar(100) DEFAULT NULL,
  `isMobile` tinyint(1) DEFAULT 0,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=572 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.event_config definition

CREATE TABLE `event_config` (
  `event_date` date DEFAULT NULL,
  `description` varchar(250) DEFAULT NULL,
  `active` tinyint(1) DEFAULT NULL,
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `startTime` varchar(100) DEFAULT NULL,
  `endTime` varchar(100) DEFAULT NULL,
  `isWebOpen` tinyint(1) DEFAULT 0,
  `code` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.meteor_showers definition

CREATE TABLE `meteor_showers` (
  `LP` int(11) NOT NULL,
  `IAUNo` varchar(10) DEFAULT NULL,
  `AdNo` varchar(10) DEFAULT NULL,
  `Code` varchar(10) DEFAULT NULL,
  `Status` int(11) DEFAULT NULL,
  `SubDate` varchar(20) DEFAULT NULL,
  `ShowerNameDesignation` varchar(100) DEFAULT NULL,
  `Activity` varchar(50) DEFAULT NULL,
  `LoSb` float DEFAULT NULL,
  `LoSe` float DEFAULT NULL,
  `LoS` float DEFAULT NULL,
  `Ra` float DEFAULT NULL,
  `De` float DEFAULT NULL,
  `dRa` float DEFAULT NULL,
  `dDe` float DEFAULT NULL,
  `Vg` float DEFAULT NULL,
  `LoR` float DEFAULT NULL,
  `S_LoR` float DEFAULT NULL,
  `LaR` float DEFAULT NULL,
  `Theta` float DEFAULT NULL,
  `Phi` float DEFAULT NULL,
  `Flags` varchar(50) DEFAULT NULL,
  `A` float DEFAULT NULL,
  `Q` float DEFAULT NULL,
  `E` float DEFAULT NULL,
  `Peri` float DEFAULT NULL,
  `Node` float DEFAULT NULL,
  `Incl` float DEFAULT NULL,
  `N` int(11) DEFAULT NULL,
  `GroupIAU` int(11) DEFAULT NULL,
  `CG` int(11) DEFAULT NULL,
  `Origin` varchar(200) DEFAULT NULL,
  `Remarks` text DEFAULT NULL,
  `OTe` varchar(10) DEFAULT NULL,
  `LookupTable` varchar(100) DEFAULT NULL,
  `ReferencesInfo` text DEFAULT NULL,
  PRIMARY KEY (`LP`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;


-- astro.pais definition

CREATE TABLE `pais` (
  `id` int(11) NOT NULL,
  `nombre` varchar(50) NOT NULL,
  `iso` char(2) NOT NULL,
  `nicename` varchar(80) NOT NULL,
  `iso3` char(3) DEFAULT NULL,
  `numcode` smallint(6) DEFAULT NULL,
  `phonecode` int(5) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `pais_unique_nombre` (`nombre`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.Informe_Fotometria definition

CREATE TABLE `Informe_Fotometria` (
  `Identificador` int(11) NOT NULL,
  `Fecha` date DEFAULT NULL,
  `Hora` varchar(200) DEFAULT NULL,
  `Estrellas_visibles` int(11) DEFAULT NULL,
  `Estrellas_usadas_para_regresion` int(11) DEFAULT NULL,
  `Coeficiente_externo_Recta_de_Bouger` decimal(18,15) DEFAULT NULL,
  `Punto_cero_Recta_de_Bouger` decimal(18,15) DEFAULT NULL,
  `Error_tipico_regresion` decimal(18,15) DEFAULT NULL,
  `Error_tipico_punto_cero` decimal(18,15) DEFAULT NULL,
  `Error_tipico_coeficiente_externo` decimal(18,15) DEFAULT NULL,
  `Coeficientes_parabola_trayectoria` varchar(200) DEFAULT NULL,
  `MagMax` decimal(21,18) DEFAULT NULL,
  `MagMin` decimal(21,18) DEFAULT NULL,
  `Masa_fotometrica` decimal(6,3) DEFAULT NULL,
  `Meteoro_Identificador` int(11) DEFAULT NULL,
  PRIMARY KEY (`Identificador`),
  KEY `Informe_Fotometria_Meteoro_FK` (`Meteoro_Identificador`),
  CONSTRAINT `Informe_Fotometria_Meteoro_FK` FOREIGN KEY (`Meteoro_Identificador`) REFERENCES `Meteoro` (`Identificador`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.Informe_Radiante definition

CREATE TABLE `Informe_Radiante` (
  `Identificador` int(11) NOT NULL,
  `Fecha` date DEFAULT NULL,
  `Hora` varchar(200) DEFAULT NULL,
  `Velocidad_Lluvia_Asociada` int(11) DEFAULT NULL,
  `Trayectorias_estimadas_para` varchar(200) DEFAULT NULL,
  `Distancia_angular_radianes` decimal(10,6) DEFAULT NULL,
  `Distancia_angular_grados` decimal(10,6) DEFAULT NULL,
  `Velocidad_angular_grad_sec` decimal(7,3) DEFAULT NULL,
  `Meteoro_Identificador` int(11) DEFAULT NULL,
  `Observatorio_Número` int(11) DEFAULT NULL,
  `Lluvia_Asociada` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`Identificador`),
  KEY `Informe_Radiante_Meteoro_FK` (`Meteoro_Identificador`),
  KEY `Informe_Radiante_Observatorio_FK` (`Observatorio_Número`),
  CONSTRAINT `Informe_Radiante_Meteoro_FK` FOREIGN KEY (`Meteoro_Identificador`) REFERENCES `Meteoro` (`Identificador`),
  CONSTRAINT `Informe_Radiante_Observatorio_FK` FOREIGN KEY (`Observatorio_Número`) REFERENCES `Observatorio` (`Número`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.Informe_Z definition

CREATE TABLE `Informe_Z` (
  `IdInforme` int(11) NOT NULL,
  `Observatorio_Número2` int(11) DEFAULT NULL,
  `Observatorio_Número` int(11) DEFAULT NULL,
  `Fecha` date DEFAULT NULL,
  `Hora` varchar(200) DEFAULT NULL,
  `Error_cuadrático_de_ortogonalidad_en_la_esfera_celeste_1` varchar(200) DEFAULT NULL,
  `Error_cuadrático_de_ortogonalidad_en_la_esfera_celeste_2` varchar(200) DEFAULT NULL,
  `Fotogramas_usados` int(11) DEFAULT NULL,
  `Ajuste_estación_2_Inicio` varchar(200) DEFAULT NULL,
  `Ajuste_estación_2_Final` varchar(200) DEFAULT NULL,
  `Ángulo_diedro_entre_planos_trayectoria` decimal(21,17) DEFAULT NULL,
  `Peso_estadístico` decimal(23,20) DEFAULT NULL,
  `Errores_AR_DE_radiante` varchar(200) DEFAULT NULL,
  `Coordenadas_astronómicas_del_radiante_Eclíptica_de_la_fecha` varchar(200) DEFAULT NULL,
  `Coordenadas_astronómicas_del_radiante_J200` varchar(200) DEFAULT NULL,
  `Azimut` decimal(15,9) DEFAULT NULL,
  `Dist_Cenital` decimal(15,9) DEFAULT NULL,
  `Inicio_de_la_trayectoria_Estacion_1` varchar(200) DEFAULT NULL,
  `Fin_de_la_trayectoria_Estacion_1` varchar(200) DEFAULT NULL,
  `Inicio_de_la_trayectoria_Estacion_2` varchar(200) DEFAULT NULL,
  `Fin_de_la_trayectoria_Estacion_2` varchar(200) DEFAULT NULL,
  `Impacto_previsible` varchar(200) DEFAULT NULL,
  `Distancia_recorrida_Estacion_1` decimal(23,18) DEFAULT NULL,
  `Error_distancia_Estacion_1` decimal(21,18) DEFAULT NULL,
  `Error_alturas_Estacion_1` decimal(21,18) DEFAULT NULL,
  `Distancia_recorrida_Estacion_2` decimal(23,18) DEFAULT NULL,
  `Error_distancia_Estacion_2` decimal(21,18) DEFAULT NULL,
  `Error_alturas_Estacion_2` decimal(21,18) DEFAULT NULL,
  `Tiempo_Estacion_1` decimal(12,9) DEFAULT NULL,
  `Velocidad_media` decimal(16,9) DEFAULT NULL,
  `Tiempo_trayectoria_en_estacion_2` decimal(12,9) DEFAULT NULL,
  `Ecuacion_del_movimiento_en_Kms` varchar(200) DEFAULT NULL,
  `Ecuacion_del_movimiento_en_gs` varchar(200) DEFAULT NULL,
  `Error_Velocidad` decimal(18,15) DEFAULT NULL,
  `Velocidad_Inicial_Estacion_2` decimal(21,15) DEFAULT NULL,
  `Aceleración_en_Kms` decimal(18,9) DEFAULT NULL,
  `Aceleración_en_gs` decimal(18,9) DEFAULT NULL,
  `Método_utilizado` int(11) DEFAULT NULL,
  `Ruta_del_informe` varchar(200) DEFAULT NULL,
  `Ecuacion_parametrica_IdEc` int(11) DEFAULT NULL,
  `Meteoro_Identificador` int(11) DEFAULT NULL,
  PRIMARY KEY (`IdInforme`),
  KEY `Informe_Z_Ecuacion_parametrica_FK` (`Ecuacion_parametrica_IdEc`),
  KEY `Informe_Z_Observatorio_FK` (`Observatorio_Número`),
  KEY `Informe_Z_Observatorio_FKv2` (`Observatorio_Número2`),
  KEY `FK_InformeZ_Meteoro_Cascade` (`Meteoro_Identificador`),
  CONSTRAINT `FK_InformeZ_Meteoro_Cascade` FOREIGN KEY (`Meteoro_Identificador`) REFERENCES `Meteoro` (`Identificador`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `Informe_Z_Ecuacion_parametrica_FK` FOREIGN KEY (`Ecuacion_parametrica_IdEc`) REFERENCES `Ecuacion_parametrica` (`IdEc`),
  CONSTRAINT `Informe_Z_Meteoro_FK` FOREIGN KEY (`Meteoro_Identificador`) REFERENCES `Meteoro` (`Identificador`),
  CONSTRAINT `Informe_Z_Observatorio_FK` FOREIGN KEY (`Observatorio_Número`) REFERENCES `Observatorio` (`Número`),
  CONSTRAINT `Informe_Z_Observatorio_FKv2` FOREIGN KEY (`Observatorio_Número2`) REFERENCES `Observatorio` (`Número`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.Lluvia_Activa_InfRad definition

CREATE TABLE `Lluvia_Activa_InfRad` (
  `Ar_de_la_fecha` decimal(7,2) DEFAULT NULL,
  `De_de_la_fecha` decimal(5,2) DEFAULT NULL,
  `Ar_más_cercano` decimal(7,2) DEFAULT NULL,
  `De_más_cercano` decimal(5,2) DEFAULT NULL,
  `Distancia` decimal(8,3) DEFAULT NULL,
  `Informe_Radiante_Identificador` int(11) NOT NULL,
  `Lluvia_Identificador` varchar(20) NOT NULL,
  `Lluvia_Año` int(11) NOT NULL,
  PRIMARY KEY (`Informe_Radiante_Identificador`,`Lluvia_Identificador`,`Lluvia_Año`),
  KEY `Lluvia_Activa_InfRad_Lluvia_FK` (`Lluvia_Identificador`,`Lluvia_Año`),
  CONSTRAINT `Lluvia_Activa_InfRad_Informe_Radiante_FK` FOREIGN KEY (`Informe_Radiante_Identificador`) REFERENCES `Informe_Radiante` (`Identificador`),
  CONSTRAINT `Lluvia_Activa_InfRad_Lluvia_FK` FOREIGN KEY (`Lluvia_Identificador`, `Lluvia_Año`) REFERENCES `Lluvia` (`Identificador`, `Año`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.Lluvia_activa definition

CREATE TABLE `Lluvia_activa` (
  `Distancia_mínima_entre_radianes_y_trayectoria` varchar(200) DEFAULT NULL,
  `Lluvia_Identificador` varchar(20) NOT NULL,
  `Lluvia_Año` int(11) DEFAULT NULL,
  `Informe_Z_IdInforme` int(11) NOT NULL,
  PRIMARY KEY (`Lluvia_Identificador`,`Informe_Z_IdInforme`),
  KEY `Lluvia_activa_Informe_Z_FK` (`Informe_Z_IdInforme`),
  KEY `Lluvia_activa_Lluvia_FK` (`Lluvia_Identificador`,`Lluvia_Año`),
  CONSTRAINT `Lluvia_activa_Informe_Z_FK` FOREIGN KEY (`Informe_Z_IdInforme`) REFERENCES `Informe_Z` (`IdInforme`),
  CONSTRAINT `Lluvia_activa_Lluvia_FK` FOREIGN KEY (`Lluvia_Identificador`, `Lluvia_Año`) REFERENCES `Lluvia` (`Identificador`, `Año`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.Puntos_ZWO definition

CREATE TABLE `Puntos_ZWO` (
  `Fecha` date DEFAULT NULL,
  `Hora` varchar(200) DEFAULT NULL,
  `X` decimal(9,4) NOT NULL,
  `Y` decimal(9,4) NOT NULL,
  `Ar_Grados` decimal(8,4) DEFAULT NULL,
  `De_Grados` decimal(8,4) DEFAULT NULL,
  `Ar_Sexagesimal` varchar(200) DEFAULT NULL,
  `De_Sexagesimal` varchar(200) DEFAULT NULL,
  `Informe_Z_IdInforme` int(11) NOT NULL,
  PRIMARY KEY (`X`,`Y`,`Informe_Z_IdInforme`),
  KEY `Puntos_ZWO_Informe_Z_FK` (`Informe_Z_IdInforme`),
  CONSTRAINT `Puntos_ZWO_Informe_Z_FK` FOREIGN KEY (`Informe_Z_IdInforme`) REFERENCES `Informe_Z` (`IdInforme`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.Puntos_del_ajuste definition

CREATE TABLE `Puntos_del_ajuste` (
  `t` decimal(6,4) NOT NULL,
  `Dist` decimal(6,2) DEFAULT NULL,
  `Mc` decimal(4,2) DEFAULT NULL,
  `Ma` decimal(4,2) DEFAULT NULL,
  `Informe_Fotometria_Identificador` int(11) NOT NULL,
  PRIMARY KEY (`t`,`Informe_Fotometria_Identificador`),
  KEY `Puntos_del_ajuste_Informe_Fotometria_FK` (`Informe_Fotometria_Identificador`),
  CONSTRAINT `Puntos_del_ajuste_Informe_Fotometria_FK` FOREIGN KEY (`Informe_Fotometria_Identificador`) REFERENCES `Informe_Fotometria` (`Identificador`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.Radiant definition

CREATE TABLE `Radiant` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `alpha` float NOT NULL,
  `delta` float NOT NULL,
  `date` date NOT NULL,
  `Identificador` varchar(50) NOT NULL,
  `Año` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_radiant_lluvia` (`Identificador`,`Año`),
  CONSTRAINT `fk_radiant_lluvia` FOREIGN KEY (`Identificador`, `Año`) REFERENCES `Lluvia` (`Identificador`, `Año`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=2861 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.Seccion definition

CREATE TABLE `Seccion` (
  `Fecha` date NOT NULL,
  `Identificador` int(11) DEFAULT NULL,
  `Ascensión_recta_del_radiante` int(11) DEFAULT NULL,
  `Declinación_del_radiante` int(11) DEFAULT NULL,
  `Lluvia_Identificador` varchar(20) NOT NULL,
  `Lluvia_Año` int(11) NOT NULL,
  PRIMARY KEY (`Fecha`,`Lluvia_Identificador`,`Lluvia_Año`),
  KEY `Seccion_Lluvia_FK` (`Lluvia_Identificador`,`Lluvia_Año`),
  CONSTRAINT `Seccion_Lluvia_FK` FOREIGN KEY (`Lluvia_Identificador`, `Lluvia_Año`) REFERENCES `Lluvia` (`Identificador`, `Año`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.Trayectoria_estimada definition

CREATE TABLE `Trayectoria_estimada` (
  `Velocidad` decimal(5,2) DEFAULT NULL,
  `Lon_Inicio` varchar(20) NOT NULL,
  `Lat_Inicio` varchar(20) DEFAULT NULL,
  `Alt_Inicio` decimal(6,2) DEFAULT NULL,
  `Dist_Inicio` decimal(6,2) DEFAULT NULL,
  `Lon_Final` varchar(20) DEFAULT NULL,
  `Lat_Final` varchar(20) DEFAULT NULL,
  `Alt_Final` decimal(6,2) DEFAULT NULL,
  `Dist_Final` decimal(6,2) DEFAULT NULL,
  `Recor` decimal(6,2) DEFAULT NULL,
  `e` decimal(6,2) DEFAULT NULL,
  `t` decimal(7,3) DEFAULT NULL,
  `Informe_Radiante_Identificador` int(11) NOT NULL,
  PRIMARY KEY (`Lon_Inicio`,`Informe_Radiante_Identificador`),
  KEY `Trayectoria_estimada_Informe_Radiante_FK` (`Informe_Radiante_Identificador`),
  CONSTRAINT `Trayectoria_estimada_Informe_Radiante_FK` FOREIGN KEY (`Informe_Radiante_Identificador`) REFERENCES `Informe_Radiante` (`Identificador`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.Trayectoria_medida definition

CREATE TABLE `Trayectoria_medida` (
  `Fecha` date DEFAULT NULL,
  `Hora` varchar(20) NOT NULL,
  `s` decimal(12,4) DEFAULT NULL,
  `t` decimal(9,6) DEFAULT NULL,
  `v` decimal(12,4) DEFAULT NULL,
  `lambda` varchar(200) DEFAULT NULL,
  `phi` varchar(200) DEFAULT NULL,
  `AR_Estacion_1` varchar(200) DEFAULT NULL,
  `De_Estacion_1` varchar(200) DEFAULT NULL,
  `Ar_Estacion_2` varchar(200) DEFAULT NULL,
  `De_Estacion_2` varchar(200) DEFAULT NULL,
  `X` decimal(9,4) DEFAULT NULL,
  `Y` decimal(9,4) DEFAULT NULL,
  `Pix` decimal(9,4) DEFAULT NULL,
  `Pix_Seg` decimal(9,4) DEFAULT NULL,
  `Informe_Z_IdInforme` int(11) NOT NULL,
  PRIMARY KEY (`Hora`,`Informe_Z_IdInforme`),
  KEY `Trayectoria_medida_Informe_Z_FK` (`Informe_Z_IdInforme`),
  CONSTRAINT `Trayectoria_medida_Informe_Z_FK` FOREIGN KEY (`Informe_Z_IdInforme`) REFERENCES `Informe_Z` (`IdInforme`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.Trayectoria_por_regresion definition

CREATE TABLE `Trayectoria_por_regresion` (
  `Fecha` date DEFAULT NULL,
  `Hora` varchar(200) DEFAULT NULL,
  `t` decimal(8,5) NOT NULL,
  `s` decimal(12,6) DEFAULT NULL,
  `v_Kms` decimal(10,3) DEFAULT NULL,
  `v_Pixs` decimal(10,3) DEFAULT NULL,
  `Informe_Z_IdInforme` int(11) NOT NULL,
  PRIMARY KEY (`t`,`Informe_Z_IdInforme`),
  KEY `Trayectoria_por_regresion_Informe_Z_FK` (`Informe_Z_IdInforme`),
  CONSTRAINT `Trayectoria_por_regresion_Informe_Z_FK` FOREIGN KEY (`Informe_Z_IdInforme`) REFERENCES `Informe_Z` (`IdInforme`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.Velociades_Angulares definition

CREATE TABLE `Velociades_Angulares` (
  `hi` decimal(5,2) NOT NULL,
  `Lluvia` decimal(6,3) DEFAULT NULL,
  `Meteoro` decimal(6,3) DEFAULT NULL,
  `Informe_Radiante_Identificador` int(11) NOT NULL,
  PRIMARY KEY (`hi`,`Informe_Radiante_Identificador`),
  KEY `Velociades_Angulares_Informe_Radiante_FK` (`Informe_Radiante_Identificador`),
  CONSTRAINT `Velociades_Angulares_Informe_Radiante_FK` FOREIGN KEY (`Informe_Radiante_Identificador`) REFERENCES `Informe_Radiante` (`Identificador`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.`user` definition

CREATE TABLE `user` (
  `id` int(11) NOT NULL,
  `email` varchar(50) NOT NULL,
  `password` varchar(100) NOT NULL,
  `name` varchar(50) NOT NULL,
  `surname` varchar(50) NOT NULL,
  `pais_id` int(11) DEFAULT NULL,
  `rol` varchar(100) DEFAULT NULL,
  `institucion` varchar(100) DEFAULT NULL,
  `is_blocked` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_unique_email` (`email`),
  KEY `user_pais_FK` (`pais_id`),
  CONSTRAINT `user_pais_FK` FOREIGN KEY (`pais_id`) REFERENCES `pais` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.user_ips definition

CREATE TABLE `user_ips` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `ip_address` varchar(45) NOT NULL,
  `region` varchar(100) DEFAULT NULL,
  `city` varchar(100) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `fk_user` (`user_id`),
  CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `user` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.Datos_meteoro_fotometria definition

CREATE TABLE `Datos_meteoro_fotometria` (
  `X_Inicio` decimal(8,3) DEFAULT NULL,
  `Y_Inicio` decimal(8,3) DEFAULT NULL,
  `Maire_Inicio` decimal(6,3) DEFAULT NULL,
  `distInicio` decimal(6,2) DEFAULT NULL,
  `X_Final` decimal(8,3) DEFAULT NULL,
  `Y_Final` decimal(8,3) DEFAULT NULL,
  `Maire_Final` decimal(6,3) DEFAULT NULL,
  `dist_Final` decimal(6,2) DEFAULT NULL,
  `Informe_Fotometria_Identificador` int(11) NOT NULL,
  PRIMARY KEY (`Informe_Fotometria_Identificador`),
  CONSTRAINT `Datos_meteoro_fotometria_Informe_Fotometria_FK` FOREIGN KEY (`Informe_Fotometria_Identificador`) REFERENCES `Informe_Fotometria` (`Identificador`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.Elementos_Orbitales definition

CREATE TABLE `Elementos_Orbitales` (
  `Informe_Z_IdInforme` int(11) NOT NULL,
  `Calculados_con` varchar(20) NOT NULL,
  `Vel__Inf` varchar(250) DEFAULT NULL,
  `Vel__Geo` varchar(250) DEFAULT NULL,
  `Ar` varchar(250) DEFAULT NULL,
  `De` varchar(250) DEFAULT NULL,
  `i` varchar(250) DEFAULT NULL,
  `p` varchar(250) DEFAULT NULL,
  `a` varchar(250) DEFAULT NULL,
  `e` varchar(250) DEFAULT NULL,
  `q` varchar(250) DEFAULT NULL,
  `T` varchar(250) DEFAULT NULL,
  `omega` varchar(250) DEFAULT NULL,
  `Omega_grados_votos_max_min` varchar(250) DEFAULT NULL,
  PRIMARY KEY (`Informe_Z_IdInforme`,`Calculados_con`),
  CONSTRAINT `Elementos_Orbitales_Informe_Z_FK` FOREIGN KEY (`Informe_Z_IdInforme`) REFERENCES `Informe_Z` (`IdInforme`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.Estrellas_usadas_para_regresión definition

CREATE TABLE `Estrellas_usadas_para_regresión` (
  `Identificador` int(11) NOT NULL,
  `Id_estrella` varchar(200) DEFAULT NULL,
  `Masa_de_aire` decimal(6,3) DEFAULT NULL,
  `Magnitud_de_catalogo` decimal(5,2) DEFAULT NULL,
  `Magnitud_instrumental` decimal(5,2) DEFAULT NULL,
  `Informe_Fotometria_Identificador` int(11) DEFAULT NULL,
  PRIMARY KEY (`Identificador`),
  KEY `Estrellas_usadas_para_regresión_Informe_Fotometria_FK` (`Informe_Fotometria_Identificador`),
  CONSTRAINT `Estrellas_usadas_para_regresión_Informe_Fotometria_FK` FOREIGN KEY (`Informe_Fotometria_Identificador`) REFERENCES `Informe_Fotometria` (`Identificador`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.password_reset_tokens definition

CREATE TABLE `password_reset_tokens` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `token` varchar(255) NOT NULL,
  `expires_at` datetime NOT NULL,
  `used` tinyint(1) DEFAULT 0,
  `created_at` datetime DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `token` (`token`),
  KEY `fk_password_reset_user` (`user_id`),
  CONSTRAINT `fk_password_reset_user` FOREIGN KEY (`user_id`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- astro.requests definition

CREATE TABLE `requests` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `requester_user_id` int(11) NOT NULL,
  `reviewer_user_id` int(11) DEFAULT NULL,
  `report_type` varchar(100) NOT NULL,
  `height` float DEFAULT NULL,
  `latitude` decimal(9,6) DEFAULT NULL,
  `longitude` decimal(9,6) DEFAULT NULL,
  `ratio` float DEFAULT NULL,
  `from_date` date NOT NULL,
  `to_date` date NOT NULL,
  `description` text DEFAULT NULL,
  `status` enum('pending','in_review','approved','rejected') DEFAULT 'pending',
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `fk_requester` (`requester_user_id`),
  KEY `fk_reviewer` (`reviewer_user_id`),
  CONSTRAINT `fk_requester` FOREIGN KEY (`requester_user_id`) REFERENCES `user` (`id`),
  CONSTRAINT `fk_reviewer` FOREIGN KEY (`reviewer_user_id`) REFERENCES `user` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
